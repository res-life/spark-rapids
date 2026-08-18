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

import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;
import org.apache.iceberg.io.IOUtil;

import java.io.IOException;
import java.util.function.ToLongFunction;

/** Shared deletion-vector byte-range reader for Iceberg versions that support v3. */
public final class IcebergDeletionVectorReader {
    private IcebergDeletionVectorReader() {}

    public static IcebergDeletionVector read(
            RapidsInputFile inputFile,
            Long offset,
            Long size,
            ToLongFunction<byte[]> cardinality) throws IOException {
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
                bytes, 8, bytes.length - 12, cardinality.applyAsLong(bytes));
    }
}
