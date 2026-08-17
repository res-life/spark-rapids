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

/*** spark-rapids-shim-json-lines
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.nio.ByteBuffer
import java.nio.file.Files

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.iceberg.{DeleteFile, FileFormat, FileMetadata, PartitionSpec}
import org.apache.iceberg.deletes.Deletes
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.io.CloseableIterable
import org.scalatest.funsuite.AnyFunSuite

class DeletionVectorReaderSuite extends AnyFunSuite {
  test("read only the deletion vector manifest byte range") {
    val expectedPositions = Seq(0L, 17L, (1L << 40) + 3L)
    val prefixBlob = serializePositions(Seq(4L, 8L))
    val targetBlob = serializePositions(expectedPositions)
    val suffixBlob = serializePositions(Seq(99L))
    val fileBytes = prefixBlob ++ targetBlob ++ suffixBlob
    val path = Files.createTempFile("iceberg-dv-range", ".puffin")

    try {
      Files.write(path, fileBytes)
      val deleteFile = deletionVectorFile(path.toUri.toString,
        referencedDataFile = "/tmp/data.parquet",
        offset = prefixBlob.length,
        size = targetBlob.length,
        cardinality = expectedPositions.size,
        fileSize = fileBytes.length)
      val inputFile = HadoopInputFile.fromPath(new HadoopPath(path.toUri), new Configuration())

      withResource(ShimUtils.readDeletionVector(deleteFile, inputFile)) { deletionVector =>
        assert(serializedBitmap(deletionVector).sameElements(portableBitmap(targetBlob)))
        assert(deletionVector.cardinality() == expectedPositions.size)
      }
    } finally {
      Files.deleteIfExists(path)
    }
  }

  test("read an empty deletion vector") {
    val blob = serializePositions(Seq.empty)
    val path = Files.createTempFile("iceberg-empty-dv", ".puffin")

    try {
      Files.write(path, blob)
      val deleteFile = deletionVectorFile(path.toUri.toString,
        referencedDataFile = "/tmp/data.parquet",
        offset = 0,
        size = blob.length,
        cardinality = 0,
        fileSize = blob.length)
      val inputFile = HadoopInputFile.fromPath(new HadoopPath(path.toUri), new Configuration())

      withResource(ShimUtils.readDeletionVector(deleteFile, inputFile)) { deletionVector =>
        val bitmap = serializedBitmap(deletionVector)
        assert(bitmap.sameElements(portableBitmap(blob)))
        assert(bitmap.length == 8)
        assert(deletionVector.cardinality() == 0)
      }
    } finally {
      Files.deleteIfExists(path)
    }
  }

  private def serializePositions(positions: Seq[Long]): Array[Byte] = {
    val boxedPositions = positions.map(Long.box).asJava
    val index = Deletes.toPositionIndex(CloseableIterable.withNoopClose(boxedPositions))
    copyBytes(index.serialize())
  }

  private def copyBytes(buffer: ByteBuffer): Array[Byte] = {
    val copy = buffer.duplicate()
    val bytes = new Array[Byte](copy.remaining())
    copy.get(bytes)
    bytes
  }

  private def portableBitmap(serializedIndex: Array[Byte]): Array[Byte] = {
    serializedIndex.slice(8, serializedIndex.length - 4)
  }

  private def serializedBitmap(deletionVector: IcebergDeletionVector): Array[Byte] = {
    withResource(deletionVector.serializedBitmap()) { bitmap =>
      val bytes = new Array[Byte](bitmap.getLength.toInt)
      bitmap.getBytes(bytes, 0, 0, bytes.length)
      bytes
    }
  }

  private def deletionVectorFile(
      location: String,
      referencedDataFile: String,
      offset: Long,
      size: Long,
      cardinality: Long,
      fileSize: Long): DeleteFile = {
    FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
      .ofPositionDeletes()
      .withPath(location)
      .withFormat(FileFormat.PUFFIN)
      .withReferencedDataFile(referencedDataFile)
      .withContentOffset(offset)
      .withContentSizeInBytes(size)
      .withRecordCount(cardinality)
      .withFileSizeInBytes(fileSize)
      .build()
  }
}
