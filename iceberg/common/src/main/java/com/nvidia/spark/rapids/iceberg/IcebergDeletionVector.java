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

/**
 * A validated Iceberg deletion vector kept in its compressed Roaring-bitmap representation.
 *
 * <p>The serialized bytes use the portable 64-bit Roaring format expected by cuDF. This object
 * owns its host buffer and must be closed after all borrowed references have been released.
 */
public final class IcebergDeletionVector implements AutoCloseable {
    private final HostMemoryBuffer serializedBitmap;
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
        this.cardinality = cardinality;
    }

    /**
     * Returns a new reference to the portable serialized 64-bit Roaring bitmap expected by cuDF.
     */
    public HostMemoryBuffer serializedBitmap() {
        serializedBitmap.incRefCount();
        return serializedBitmap;
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
