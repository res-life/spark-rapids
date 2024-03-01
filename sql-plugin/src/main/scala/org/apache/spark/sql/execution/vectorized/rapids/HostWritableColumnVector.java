/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.vectorized.rapids;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import ai.rapids.cudf.*;
import com.nvidia.spark.rapids.GpuColumnVector;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

public class HostWritableColumnVector extends WritableColumnVector {

	private HostMemoryBuffer data = null;
	private HostMemoryBuffer valids = null;
	// offset for ArrayType | MapType
	private HostMemoryBuffer arrayOffsets = null;
	// offset for StringType
	private HostMemoryBuffer charOffsets = null;
	private int lastCharRowId = -1;

	private int rowGroupOffset = 0;
	private int rowGroupArrayOffset = 0;
	private int rowGroupStringOffset = 0;
	private int rowGroupIndex = 0;
	private int rowGroupArrayIndex = 0;

	public HostWritableColumnVector(int capacity, DataType type) {
		super(capacity, type);
		this.capacity = 0;
		resetAllBuffers(capacity, false);
	}

	public HostColumnVector build() {
		assert rowGroupIndex == 0 && rowGroupArrayIndex == 0;

		return (HostColumnVector) buildImpl(true,
				new Random().nextInt(Integer.MAX_VALUE - 1));
	}

	private HostColumnVectorCore buildImpl(boolean topLevel, int rdSeed) {
		// Make sure all subVectors are fully "flushed" before the materialization.
		assert rowGroupIndex == 0 && rowGroupArrayIndex == 0;

		// System.err.format("[seed=%d]building %s(rowCnt=%d,arrayOffset=%d,charOffset=%d,capacity=%d)\n",
		// 			rdSeed, type, rowGroupOffset, rowGroupArrayOffset, rowGroupStringOffset, capacity);

		DType cudfType = type instanceof MapType ? DType.LIST : GpuColumnVector.getRapidsType(type);

		HostMemoryBuffer offsetBuffer = null;
		HostMemoryBuffer validBuffer = null;
		HostMemoryBuffer dataBuffer = null;
		Optional<Long> nullCnt = Optional.of((long) numNulls);

		if (type instanceof MapType || type instanceof ArrayType) {
			offsetBuffer = arrayOffsets;
			arrayOffsets = null;
		} else if (type instanceof StringType) {
			// padding tail offsets
			if (lastCharRowId + 1 < rowGroupOffset) {
				int byteArrayEnd = charOffsets.getInt((lastCharRowId + 1) * 4L);
				for (int i = lastCharRowId + 2; i < rowGroupOffset + 1; ++i)
					charOffsets.setInt(i * 4L, byteArrayEnd);
			}
			offsetBuffer = charOffsets;
			charOffsets = null;
			dataBuffer = ((HostWritableColumnVector)childColumns[0]).data;
			((HostWritableColumnVector)childColumns[0]).data = null;
		} else if (type instanceof StructType) {

		} else {
			dataBuffer = data;
			data = null;
		}

		// Build bitwise validity mask
		if (valids != null) {
			validBuffer = buildNullMask(valids, rowGroupOffset);
			 valids.close();
			 valids = null;
		}

		// Build child columns of "real" nested types recursively (StringType is NOT even it contains childColumns)
		List<HostColumnVectorCore> children = new ArrayList<>();
		if (childColumns != null && !(type instanceof StringType)) {
			for (WritableColumnVector ch : childColumns) {
				children.add(((HostWritableColumnVector) ch).buildImpl(false, rdSeed));
			}
		}

		// Wrap the level of Struct to adapt cuDF layout for MapType
		// Array[(keyCol, valueCol)] => Array[Struct[keyCol, valueCol]]
		if (type instanceof MapType) {
			HostColumnVectorCore cv = new HostColumnVectorCore(
					DType.STRUCT,
					children.get(0).getRowCount(),
					Optional.of(0L),
					null, null, null, children);
			children = new ArrayList<>();
			children.add(cv);
		}

		if (topLevel) {
			return new HostColumnVector(
					cudfType, rowGroupOffset, nullCnt, dataBuffer, validBuffer, offsetBuffer, children);
		}
		return new HostColumnVectorCore(
				cudfType, rowGroupOffset, nullCnt, dataBuffer, validBuffer, offsetBuffer, children);
	}

	private HostMemoryBuffer buildNullMask(HostMemoryBuffer byteMask, int numRecord) {
		long actualBytes = ((long) numRecord + 7) >> 3;
		long paddingBytes = ((actualBytes + 63) >> 6) << 6;
		HostMemoryBuffer bitMask = HostMemoryBuffer.allocate(paddingBytes);
		for (int i = 0; i < numRecord - 7; i += 8) {
			int mask = (byteMask.getByte(i) ^ 1)
					| ((byteMask.getByte(i + 1) ^ 1) << 1)
					| ((byteMask.getByte(i + 2) ^ 1) << 2)
					| ((byteMask.getByte(i + 3) ^ 1) << 3)
					| ((byteMask.getByte(i + 4) ^ 1) << 4)
					| ((byteMask.getByte(i + 5) ^ 1) << 5)
					| ((byteMask.getByte(i + 6) ^ 1) << 6)
					| ((byteMask.getByte(i + 7) ^ 1) << 7);
			bitMask.setByte(i >> 3, (byte) mask);
		}
		int lastByte = 0;
		int j = 0;
		for (int i = (numRecord >> 3) << 3; i < numRecord; i++) {
			lastByte |= ((byteMask.getByte(i) ^ 1) << j++);
		}
		if (j > 0) {
			bitMask.setByte(numRecord >> 3, (byte) lastByte);
		}

		return bitMask;
	}

	@Override
	public void reset() {
		if (childColumns != null) {
			for (WritableColumnVector c: childColumns)
				c.reset();
		}
		rowGroupOffset += rowGroupIndex;
		rowGroupIndex = 0;
		rowGroupArrayOffset += rowGroupArrayIndex;
		rowGroupArrayIndex = 0;
		if (lastCharRowId > -1) {
			rowGroupStringOffset = charOffsets.getInt((lastCharRowId + 1) * 4L);
		}
		elementsAppended = 0;
	}

	public void reallocate(int newCapacity) {
		this.capacity = 0;
		this.elementsAppended = 0;
		this.numNulls = 0;

		lastCharRowId = -1;

		this.rowGroupIndex = 0;
		this.rowGroupArrayIndex = 0;
		this.rowGroupOffset = 0;
		this.rowGroupArrayOffset = 0;
		this.rowGroupStringOffset = 0;

		if (newCapacity > 0) {
			resetAllBuffers(newCapacity, false);
		}

		if (childColumns != null) {
			if (isArray() && !(type instanceof ArrayType)) {
				newCapacity *= DEFAULT_ARRAY_LENGTH;
			}
			for (WritableColumnVector ch : childColumns) {
				((HostWritableColumnVector) ch).reallocate(newCapacity);
			}
		}
	}

	@Override
	public void putBooleans(int rowId, byte src) {
		rowGroupIndex += 8;
		rowId += rowGroupOffset;
		data.setByte(rowId, (byte)(src & 1));
		data.setByte(rowId + 1, (byte)(src >>> 1 & 1));
		data.setByte(rowId + 2, (byte)(src >>> 2 & 1));
		data.setByte(rowId + 3, (byte)(src >>> 3 & 1));
		data.setByte(rowId + 4, (byte)(src >>> 4 & 1));
		data.setByte(rowId + 5, (byte)(src >>> 5 & 1));
		data.setByte(rowId + 6, (byte)(src >>> 6 & 1));
		data.setByte(rowId + 7, (byte)(src >>> 7 & 1));
	}

	@Override
	public boolean isNullAt(int rowId) {
		if (isAllNull) return true;
		if (valids == null) return false;
		return valids.getByte(rowGroupOffset + rowId) == 1;
	}

	@Override
	public void putNotNull(int rowId) {
		rowGroupIndex++;
		if (!hasNull() || valids == null) return;
		valids.setByte(rowGroupOffset + rowId, (byte) 0);
	}

	@Override
	public void putNull(int rowId) {
		rowGroupIndex++;
		if (valids == null) {
			allocateNullVector(capacity, true);
		}
		valids.setByte(rowGroupOffset + rowId, (byte) 1);
		++numNulls;
	}

	@Override
	public void putNulls(int rowId, int count) {
		rowGroupIndex += count;
		if (valids == null) {
			allocateNullVector(capacity, true);
		}
		valids.setMemory(rowGroupOffset + rowId, count, (byte) 1);
		numNulls += count;
	}

	@Override
	public void putNotNulls(int rowId, int count) {
		// rowCnt = Math.max(rowCnt, rowId + count);
		if (!hasNull() || valids == null) return;
		valids.setMemory(rowGroupOffset + rowId, count, (byte) 0);
	}

	@Override
	public void putBoolean(int rowId, boolean value) {
		rowGroupIndex++;
		data.setBoolean(rowGroupOffset + rowId, value);
	}

	@Override
	public void putBooleans(int rowId, int count, boolean value) {
		rowGroupIndex += count;
		data.setMemory(rowGroupOffset + rowId, count, value ? (byte) 1 : (byte) 0);
	}

	@Override
	public void putByte(int rowId, byte value) {
		rowGroupIndex++;
		data.setByte(rowGroupOffset + rowId, value);
	}

	@Override
	public void putBytes(int rowId, int count, byte value) {
		rowGroupIndex += count;
		data.setMemory(rowGroupOffset + rowId, count, value);
	}

	@Override
	public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
		rowGroupIndex += count;
		data.setBytes(rowGroupOffset + rowId, src, srcIndex, count);
	}

	@Override
	public void putShort(int rowId, short value) {
		rowGroupIndex++;
		rowId += rowGroupOffset;
		data.setShort(rowId * 2L, value);
	}

	@Override
	public void putShorts(int rowId, int count, short value) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		for (int offset = rowId; offset < rowId + count; offset++) {
			data.setShort(offset * 2L, value);
		}
	}

	@Override
	public void putShorts(int rowId, int count, short[] src, int srcIndex) {
		rowGroupIndex++;
		rowId += rowGroupOffset;
		data.setShorts(rowId * 2L, src, srcIndex, count);
	}

	@Override
	public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setBytes(rowId * 2L, src, srcIndex, count * 2L);
	}

	@Override
	public void putInt(int rowId, int value) {
		rowGroupIndex++;
		rowId += rowGroupOffset;
		data.setInt(rowId * 4L, value);
	}

	@Override
	public void putInts(int rowId, int count, int value) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		for (int offset = rowId; offset < rowId + count; offset++) {
			data.setInt(offset * 4L, value);
		}
	}

	@Override
	public void putInts(int rowId, int count, int[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setInts(rowId * 4L, src, srcIndex, count);
	}

	@Override
	public void putInts(int rowId, int count, byte[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setBytes(rowId * 4L, src, srcIndex, count * 4L);
	}

	@Override
	public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
		putInts(rowId, count, src, srcIndex);
	}

	@Override
	public void putLong(int rowId, long value) {
		rowGroupIndex++;
		rowId += rowGroupOffset;
		data.setLong(rowId * 8L, value);
	}

	@Override
	public void putLongs(int rowId, int count, long value) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		for (int offset = rowId; offset < rowId + count; offset++) {
			data.setLong(offset * 8L, value);
		}
	}

	@Override
	public void putLongs(int rowId, int count, long[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setLongs(rowId * 8L, src, srcIndex, count);
	}

	@Override
	public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setBytes(rowId * 8L, src, srcIndex, count * 8L);
	}

	@Override
	public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
		putLongs(rowId, count, src, srcIndex);
	}

	@Override
	public void putFloat(int rowId, float value) {
		rowGroupIndex++;
		rowId += rowGroupOffset;
		data.setFloat(rowId * 4L, value);
	}

	@Override
	public void putFloats(int rowId, int count, float value) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		for (int offset = rowId; offset < rowId + count; offset++) {
			data.setFloat(offset * 4L, value);
		}
	}

	@Override
	public void putFloats(int rowId, int count, float[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setFloats(rowId * 4L, src, srcIndex, count);
	}

	@Override
	public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setBytes(rowId * 4L, src, srcIndex, count * 4L);
	}

	@Override
	public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
		putFloats(rowId, count, src, srcIndex);
	}

	@Override
	public void putDouble(int rowId, double value) {
		rowGroupIndex++;
		rowId += rowGroupOffset;
		data.setDouble(rowId * 8L, value);
	}

	@Override
	public void putDoubles(int rowId, int count, double value) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		for (int offset = rowId; offset < rowId + count; offset++) {
			data.setDouble(offset * 8L, value);
		}
	}

	@Override
	public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setDoubles(rowId * 8L, src, srcIndex, count);
	}

	@Override
	public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
		rowGroupIndex += count;
		rowId += rowGroupOffset;
		data.setBytes(rowId * 8L, src, srcIndex, count * 8L);
	}

	@Override
	public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
		putDoubles(rowId, count, src, srcIndex);
	}

	@Override
	public void putArray(int rowId, int offset, int length) {
		rowGroupArrayIndex += length;
		arrayOffsets.setInt((rowGroupOffset + rowId + 1) * 4L, rowGroupArrayOffset + rowGroupArrayIndex);
	}

	@Override
	public int putByteArray(int rowId, byte[] value, int offset, int length) {
		rowGroupIndex++;
		rowId += rowGroupOffset;

		int result = arrayData().appendBytes(length, value, offset) + rowGroupStringOffset;

		for (int i = lastCharRowId + 1; i < rowId; ++i) {
			charOffsets.setInt((i + 1) * 4L, result);
		}
		charOffsets.setInt((rowId + 1) * 4L, result + length);
		lastCharRowId = rowId;

		return result;
	}

	@Override
	public void reserve(int requiredCapacity) {
		super.reserve(requiredCapacity + rowGroupOffset);
	}

	@Override
	protected void reserveInternal(int newCap) {
		resetAllBuffers(newCap, true);
	}

	private void resetAllBuffers(int newCap, boolean keepData) {
		if (valids != null) {
			allocateNullVector(newCap, keepData);
		}

		if (type instanceof ArrayType || type instanceof MapType) {
			arrayOffsets = transferBuffer((newCap + 1) * 4L, arrayOffsets, keepData);
			arrayOffsets.setInt(0, 0);
		} else if (isArray()) {
			charOffsets = transferBuffer((newCap + 1) * 4L, charOffsets, keepData);
			charOffsets.setInt(0, 0);
		} else if (type instanceof ByteType || type instanceof BooleanType) {
			data = transferBuffer(newCap, data, keepData);
		} else if (type instanceof ShortType) {
			data = transferBuffer(newCap * 2L, data, keepData);
		} else if (type instanceof IntegerType || type instanceof FloatType ||
				type instanceof DateType || DecimalType.is32BitDecimalType(type) ||
				type instanceof YearMonthIntervalType) {
			data = transferBuffer(newCap * 4L, data, keepData);
		} else if (type instanceof LongType || type instanceof DoubleType ||
				DecimalType.is64BitDecimalType(type) || type instanceof TimestampType ||
				type instanceof TimestampNTZType || type instanceof DayTimeIntervalType) {
			data = transferBuffer(newCap * 8L, data, keepData);
		} else if (childColumns != null) {
			// Nothing to store.
		} else {
			throw new RuntimeException("Unhandled " + type);
		}

		capacity = newCap;
	}

	private void allocateNullVector(int capacity, boolean keepData) {
		long currentSize = valids == null ? 0 : valids.getLength();

		valids = transferBuffer(capacity, valids, keepData);

		if (!keepData) {
			valids.setMemory(0, capacity, (byte) 0);
		} else if (currentSize < capacity) {
			valids.setMemory(currentSize, capacity - currentSize, (byte) 0);
		}
	}

	@Override
	protected WritableColumnVector reserveNewColumn(int capacity, DataType type) {
		return new HostWritableColumnVector(capacity, type);
	}

	@Override
	public WritableColumnVector reserveDictionaryIds(int capacity) {
		if (dictionaryIds == null) {
			dictionaryIds = new ParquetHelperVector(capacity, ParquetHelperVector.PinMode.SYSTEM_DEFAULT);
		} else {
			ParquetHelperVector ids = (ParquetHelperVector) dictionaryIds;
			ids.elementsAppended = 0;
			ids.resetBuffer(capacity, false);
		}
		return dictionaryIds;
	}

	private HostMemoryBuffer transferBuffer(long targetSize, HostMemoryBuffer buffer, boolean keepData) {
		assert targetSize > 0;
		long currentSize = buffer == null ? 0L : buffer.getLength();

		if (currentSize == targetSize) {
			return buffer;
		}

		if (currentSize > targetSize) {
			if (keepData) {
				throw new RuntimeException("Can NOT keep data because targetSize < currentSize");
			}
			HostMemoryBuffer sliced = buffer.slice(0, targetSize);
			buffer.close();
			return sliced;
		}

		HostMemoryBuffer extended = HostMemoryBuffer.allocate(targetSize);
		if (currentSize > 0) {
			if (keepData) {
				extended.copyFromHostBuffer(0, buffer, 0, currentSize);
			}
			buffer.close();
		}

		return extended;
	}

	@Override
	public int getDictId(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	protected UTF8String getBytesAsUTF8String(int rowId, int count) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public int getArrayLength(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public int getArrayOffset(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public boolean getBoolean(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public byte getByte(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public short getShort(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public int getInt(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public long getLong(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public float getFloat(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public double getDouble(int rowId) {
		throw new UnsupportedOperationException("RapidsWritableColumnVector does NOT support getters");
	}

	@Override
	public ByteBuffer getByteBuffer(int rowId, int count) {
		byte[] buffer = new byte[count];
		data.getBytes(buffer, count, rowId, count);
		return ByteBuffer.wrap(buffer);
	}

	@Override
	public void close() {
		if (arrayOffsets != null) {
			while (arrayOffsets.getRefCount() > 0)
				arrayOffsets.close();
		}
		if (data != null) {
			while (data.getRefCount() > 0)
				data.close();
		}
		if (charOffsets != null) {
			while (charOffsets.getRefCount() > 0)
				charOffsets.close();
		}
		if (valids != null) {
			while (valids.getRefCount() > 0)
				valids.close();
		}

		super.close();
	}

	private void dumpOffsetVector(HostMemoryBuffer offsetBuffer, int rdSeed) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("\n[").append(rdSeed).append("]offsetVector: ");
		for (int i = 0; i < offsetBuffer.getLength(); i += 4) {
			buffer.append(offsetBuffer.getInt(i)).append(", ");
		}
		System.err.println(buffer);
	}

	private void dumpLongVector(HostMemoryBuffer buffer, int rdSeed) {
		StringBuffer sb = new StringBuffer();
		sb.append('[').append(rdSeed).append("] LongBuffer: ");
		for (int i = 0; i < buffer.getLength(); i += 8)
			sb.append(buffer.getLong(i)).append(", ");
		System.err.println(sb);
	}

	private void dumpStringVector(HostMemoryBuffer offsetBuffer, HostMemoryBuffer dataBuffer, int rdSeed) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("\n[").append(rdSeed).append("]stringOffset: ");
		for (int i = 0; i < offsetBuffer.getLength(); i += 4) {
			buffer.append(offsetBuffer.getInt(i)).append(", ");
		}
		buffer.append("\nstringVector: ");
		for (int i = 0; i < offsetBuffer.getLength() - 4; i += 4) {
			for (int j = offsetBuffer.getInt(i); j < offsetBuffer.getInt(i + 4); ++j) {
				if (j >= dataBuffer.getLength()) {
					buffer.append("+++");
					System.err.println(buffer);
					return;
				}
				buffer.append((char) dataBuffer.getByte(j));
			}
			buffer.append(", ");
		}
		System.err.println(buffer);
	}
}
