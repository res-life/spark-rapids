/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;
import org.apache.iceberg.io.IOUtil;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * An Iceberg deletion vector kept in its compressed Roaring-bitmap representation.
 *
 * <p>The serialized bytes use the portable 64-bit Roaring format expected by cuDF. This object
 * owns its host buffer and must be closed after all borrowed references have been released.
 */
public final class IcebergDeletionVector implements AutoCloseable {
    private final HostMemoryBuffer serializedBitmap;
    private final long serializedSizeInBytes;
    private final long cardinality;

    public IcebergDeletionVector(
            byte[] serializedIndex,
            int bitmapOffset,
            int bitmapLength,
            long cardinality) {
        HostMemoryBuffer bitmap = HostMemoryBuffer.allocate(bitmapLength);
        try {
            bitmap.setBytes(0, serializedIndex, bitmapOffset, bitmapLength);
        } catch (RuntimeException | Error e) {
            bitmap.close();
            throw e;
        }
        this.serializedBitmap = bitmap;
        this.serializedSizeInBytes = serializedIndex.length;
        this.cardinality = cardinality;
    }

    /** Reads an Iceberg deletion-vector byte range. */
    public static IcebergDeletionVector read(
            RapidsInputFile inputFile,
            Long offset,
            Long size,
            long cardinality) throws IOException {
        if (offset == null || offset < 0) {
            throw new IllegalArgumentException("Invalid deletion vector offset: " + offset);
        }
        if (size == null || size < 20 || size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid deletion vector size: " + size);
        }

        byte[] bytes = new byte[size.intValue()];
        try (SeekableInputStream stream = inputFile.open()) {
            stream.seek(offset);
            IOUtil.readFully(stream, bytes, 0, bytes.length);
        }

        return new IcebergDeletionVector(
                bytes, 8, bytes.length - 12, cardinality);
    }

    /**
     * Returns the portable serialized 64-bit Roaring bitmap expected by cuDF.
     *
     * <p>The returned buffer is owned by this object. Callers that retain it must increment its
     * reference count.
     */
    public HostMemoryBuffer serializedBitmap() {
        return serializedBitmap;
    }

    /** Returns the full serialized deletion-vector size, including its header and checksum. */
    public long serializedSizeInBytes() {
        return serializedSizeInBytes;
    }

    /** Returns the number of positions in the deletion vector. */
    public long cardinality() {
        return cardinality;
    }

    /**
     * Returns the positions encoded by this deletion vector.
     *
     * <p>This is used when Iceberg projects {@code _deleted}: the Parquet reader must retain all
     * rows, so the deletion vector is applied after the read to mark rows instead of removing
     * them. Iceberg serializes a 64-bit bitmap as a little-endian sequence of keyed 32-bit
     * Roaring bitmaps.
     */
    public long[] deletedPositions() throws IOException {
        if (cardinality > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Cannot materialize a deletion vector with more than Integer.MAX_VALUE rows");
        }

        byte[] bytes = new byte[(int) serializedBitmap.getLength()];
        serializedBitmap.getBytes(bytes, 0, 0, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        long bitmapCount = buffer.getLong();
        if (bitmapCount < 0 || bitmapCount > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Invalid deletion-vector bitmap count: " + bitmapCount);
        }

        long[] positions = new long[(int) cardinality];
        int positionIndex = 0;
        int lastKey = -1;
        for (int i = 0; i < (int) bitmapCount; i++) {
            int key = buffer.getInt();
            if (key < 0 || key <= lastKey) {
                throw new IllegalArgumentException("Invalid deletion-vector bitmap key: " + key);
            }

            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.deserialize(buffer);
            buffer.position(buffer.position() + bitmap.serializedSizeInBytes());
            IntIterator iterator = bitmap.getIntIterator();
            while (iterator.hasNext()) {
                if (positionIndex == positions.length) {
                    throw new IllegalArgumentException(
                            "Deletion-vector cardinality is smaller than its serialized bitmap");
                }
                positions[positionIndex] =
                        (((long) key) << 32) | Integer.toUnsignedLong(iterator.next());
                positionIndex += 1;
            }
            lastKey = key;
        }

        if (positionIndex != positions.length) {
            throw new IllegalArgumentException(
                    "Deletion-vector cardinality does not match its serialized bitmap: expected "
                            + positions.length + ", found " + positionIndex);
        }
        return positions;
    }

    @Override
    public void close() {
        serializedBitmap.close();
    }
}
