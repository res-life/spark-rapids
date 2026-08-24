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

/**
 * An Iceberg deletion vector kept in its compressed Roaring-bitmap representation.
 *
 * <p>The serialized bytes use the portable 64-bit Roaring format expected by cuDF. This object
 * owns its host buffer and must be closed after all borrowed references have been released.
 */
public final class IcebergDeletionVector implements AutoCloseable {
    private static final int BITMAP_OFFSET_BYTES = 8;
    private static final int ENVELOPE_SIZE_BYTES = 12;
    private static final int MINIMUM_SIZE_BYTES = 20;
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
            // Iceberg wraps the portable Roaring bitmap with an 8-byte header and a 4-byte CRC.
            // cuDF accepts only the bitmap, so copy just that range instead of retaining a slice
            // of a full-size host buffer. Use bounded heap staging to avoid a large byte[] for DVs.
            stream.seek(offset + BITMAP_OFFSET_BYTES);
            int bitmapLength = Math.toIntExact(size - ENVELOPE_SIZE_BYTES);
            HostMemoryBuffer bitmap = HostMemoryBuffer.allocate(bitmapLength);
            try {
                byte[] staging = new byte[Math.min(STAGING_BUFFER_SIZE_BYTES, bitmapLength)];
                int remaining = bitmapLength;
                long bitmapOffset = 0;
                while (remaining > 0) {
                    int bytesToRead = Math.min(staging.length, remaining);
                    IOUtil.readFully(stream, staging, 0, bytesToRead);
                    bitmap.setBytes(bitmapOffset, staging, 0, bytesToRead);
                    bitmapOffset += bytesToRead;
                    remaining -= bytesToRead;
                }
                // Consume the CRC so a truncated deletion-vector range still fails here.
                IOUtil.readFully(stream, new byte[Integer.BYTES], 0, Integer.BYTES);
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
