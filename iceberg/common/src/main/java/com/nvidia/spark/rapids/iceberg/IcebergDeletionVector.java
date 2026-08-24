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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * An Iceberg deletion vector kept in its compressed Roaring-bitmap representation.
 *
 * <p>The serialized bytes use the portable 64-bit Roaring format expected by cuDF. This object
 * owns its host buffer and must be closed after all borrowed references have been released.
 */
public final class IcebergDeletionVector implements AutoCloseable {
    private static final int LENGTH_SIZE_BYTES = 4;
    private static final int MAGIC_NUMBER_SIZE_BYTES = 4;
    private static final int CRC_SIZE_BYTES = 4;
    private static final int MINIMUM_SIZE_BYTES = 20;
    private static final int MAGIC_NUMBER = 1681511377;
    private static final int STAGING_BUFFER_SIZE_BYTES = 64 * 1024;

    private final HostMemoryBuffer serializedBitmap;
    private final long serializedSizeInBytes;
    private final long cardinality;

    IcebergDeletionVector(
            HostMemoryBuffer serializedBitmap,
            long serializedSizeInBytes,
            long cardinality) {
        this.serializedBitmap = serializedBitmap;
        this.serializedSizeInBytes = serializedSizeInBytes;
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
        if (size == null || size < MINIMUM_SIZE_BYTES || size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid deletion vector size: " + size);
        }
        if (offset > Long.MAX_VALUE - size) {
            throw new IllegalArgumentException(
                    "Invalid deletion vector range: offset=" + offset + ", size=" + size);
        }

        try (SeekableInputStream stream = inputFile.open()) {
            stream.seek(offset);
            byte[] header = new byte[LENGTH_SIZE_BYTES + MAGIC_NUMBER_SIZE_BYTES];
            IOUtil.readFully(stream, header, 0, header.length);

            int bitmapDataLength = ByteBuffer.wrap(header).getInt();
            long expectedBitmapDataLength = size - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
            if (bitmapDataLength != expectedBitmapDataLength) {
                throw new IllegalArgumentException(
                        "Invalid deletion-vector bitmap data length: " + bitmapDataLength
                                + ", expected " + expectedBitmapDataLength);
            }

            int magicNumber = ByteBuffer.wrap(header)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt(LENGTH_SIZE_BYTES);
            if (magicNumber != MAGIC_NUMBER) {
                throw new IllegalArgumentException(
                        "Invalid deletion-vector magic number: " + magicNumber
                                + ", expected " + MAGIC_NUMBER);
            }

            int bitmapLength = bitmapDataLength - MAGIC_NUMBER_SIZE_BYTES;
            HostMemoryBuffer bitmap = HostMemoryBuffer.allocate(bitmapLength);
            try {
                CRC32 crc = new CRC32();
                crc.update(header, LENGTH_SIZE_BYTES, MAGIC_NUMBER_SIZE_BYTES);

                byte[] staging = new byte[Math.min(STAGING_BUFFER_SIZE_BYTES, bitmapLength)];
                int remaining = bitmapLength;
                long bitmapOffset = 0;
                while (remaining > 0) {
                    int bytesToRead = Math.min(staging.length, remaining);
                    IOUtil.readFully(stream, staging, 0, bytesToRead);
                    bitmap.setBytes(bitmapOffset, staging, 0, bytesToRead);
                    crc.update(staging, 0, bytesToRead);
                    bitmapOffset += bytesToRead;
                    remaining -= bytesToRead;
                }

                byte[] expectedCrcBytes = new byte[CRC_SIZE_BYTES];
                IOUtil.readFully(stream, expectedCrcBytes, 0, expectedCrcBytes.length);
                int expectedCrc = ByteBuffer.wrap(expectedCrcBytes).getInt();
                int actualCrc = (int) crc.getValue();
                if (actualCrc != expectedCrc) {
                    throw new IllegalArgumentException(
                            "Invalid deletion-vector CRC: " + actualCrc
                                    + ", expected " + expectedCrc);
                }

                return new IcebergDeletionVector(bitmap, size, cardinality);
            } catch (IOException | RuntimeException | Error e) {
                bitmap.close();
                throw e;
            }
        }
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

    @Override
    public void close() {
        serializedBitmap.close();
    }
}
