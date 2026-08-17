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

/**
 * A validated Iceberg deletion vector kept in its compressed Roaring-bitmap representation.
 *
 * <p>The serialized bytes use the portable 64-bit Roaring format expected by cuDF. Range
 * counting is provided by the version-specific Iceberg implementation so the common module does
 * not depend on deletion-index APIs that are absent from Iceberg 1.6.
 */
public interface IcebergDeletionVector {
    /** Returns the portable serialized 64-bit Roaring bitmap expected by cuDF. */
    byte[] serializedBitmap();

    /** Returns the number of positions in the deletion vector. */
    long cardinality();

    /** Returns the number of deleted positions contained in the supplied file-row ranges. */
    long countDeletedRows(long[] rowGroupOffsets, int[] rowGroupNumRows);
}
