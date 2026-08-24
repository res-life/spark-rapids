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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.io.IOException
import java.nio.{ByteBuffer, ByteOrder}
import java.util.OptionalLong
import java.util.zip.CRC32

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.fileio.{RapidsInputFile, SeekableInputStream}
import org.scalatest.funsuite.AnyFunSuite

class IcebergDeletionVectorSuite extends AnyFunSuite {
  private val MagicNumber = 1681511377
  private val StagingBufferSizeBytes = 64 * 1024

  private class ByteArraySeekableInputStream(bytes: Array[Byte]) extends SeekableInputStream {
    private var pos = 0
    var maxRequestedReadLength = 0

    override def read(): Int = {
      if (pos >= bytes.length) {
        -1
      } else {
        val result = bytes(pos) & 0xff
        pos += 1
        result
      }
    }

    override def read(output: Array[Byte], offset: Int, length: Int): Int = {
      maxRequestedReadLength = math.max(maxRequestedReadLength, length)
      if (pos >= bytes.length) {
        -1
      } else {
        val readLength = math.min(length, bytes.length - pos)
        System.arraycopy(bytes, pos, output, offset, readLength)
        pos += readLength
        readLength
      }
    }

    override def getPos: Long = pos

    override def seek(newPos: Long): Unit = {
      pos = Math.toIntExact(newPos)
    }
  }

  private class ByteArrayInputFile(bytes: Array[Byte]) extends RapidsInputFile {
    var openCount = 0
    var stream: ByteArraySeekableInputStream = _

    override def getLength: Long = bytes.length

    override def getLastModificationTime: OptionalLong = OptionalLong.empty()

    override def open(): SeekableInputStream = {
      openCount += 1
      stream = new ByteArraySeekableInputStream(bytes)
      stream
    }
  }

  private def serializeEnvelope(bitmap: Array[Byte]): Array[Byte] = {
    val bitmapData = ByteBuffer.allocate(Integer.BYTES + bitmap.length)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putInt(MagicNumber)
      .put(bitmap)
      .array()
    val crc = new CRC32()
    crc.update(bitmapData)
    ByteBuffer.allocate(Integer.BYTES + bitmapData.length + Integer.BYTES)
      .putInt(bitmapData.length)
      .put(bitmapData)
      .putInt(crc.getValue.toInt)
      .array()
  }

  test("read a deletion vector from a non-zero offset with bounded staging") {
    val bitmap = Array.tabulate[Byte](128 * 1024 + 7)(i => i.toByte)
    val envelope = serializeEnvelope(bitmap)
    val prefix = Array.fill[Byte](17)(1)
    val suffix = Array.fill[Byte](23)(2)
    val inputFile = new ByteArrayInputFile(prefix ++ envelope ++ suffix)

    withResource(IcebergDeletionVector.read(
        inputFile, prefix.length.toLong, envelope.length.toLong, 123L)) { vector =>
      val actual = new Array[Byte](bitmap.length)
      vector.serializedBitmap().getBytes(actual, 0, 0, actual.length)
      assert(actual.sameElements(bitmap))
      assert(vector.serializedSizeInBytes() == envelope.length)
      assert(vector.cardinality() == 123L)
      assert(inputFile.stream.getPos == prefix.length + envelope.length)
      assert(inputFile.stream.maxRequestedReadLength <= StagingBufferSizeBytes)
    }
  }

  test("read the minimum-size empty deletion vector") {
    val bitmap = ByteBuffer.allocate(java.lang.Long.BYTES)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putLong(0L)
      .array()
    val envelope = serializeEnvelope(bitmap)
    assert(envelope.length == 20)

    withResource(IcebergDeletionVector.read(
        new ByteArrayInputFile(envelope), 0L, envelope.length.toLong, 0L)) { vector =>
      assert(vector.serializedBitmap().getLength == java.lang.Long.BYTES)
    }
  }

  test("reject truncated deletion-vector input") {
    val envelope = serializeEnvelope(Array.fill[Byte](32)(3))
    val truncated = envelope.dropRight(1)
    assertThrows[IOException] {
      IcebergDeletionVector.read(
        new ByteArrayInputFile(truncated), 0L, envelope.length.toLong, 1L)
    }
  }

  test("reject invalid deletion-vector envelopes") {
    val envelope = serializeEnvelope(Array.fill[Byte](32)(3))
    val invalidLength = envelope.clone()
    ByteBuffer.wrap(invalidLength).putInt(envelope.length)
    val invalidMagic = envelope.clone()
    ByteBuffer.wrap(invalidMagic).order(ByteOrder.LITTLE_ENDIAN)
      .putInt(Integer.BYTES, MagicNumber + 1)
    val invalidCrc = envelope.clone()
    invalidCrc(invalidCrc.length - 1) = (invalidCrc.last ^ 0xff).toByte

    Seq(
      (invalidLength, "Invalid bitmap data length"),
      (invalidMagic, "Invalid magic number"),
      (invalidCrc, "Invalid CRC")
    ).foreach { case (bytes, expectedMessage) =>
      val error = intercept[IOException] {
        IcebergDeletionVector.read(
          new ByteArrayInputFile(bytes), 0L, bytes.length.toLong, 1L)
      }
      assert(error.getMessage.contains(expectedMessage))
    }
  }

  test("reject invalid deletion-vector ranges before opening the file") {
    val inputFile = new ByteArrayInputFile(Array.emptyByteArray)
    val invalidRanges = Seq[(java.lang.Long, java.lang.Long)](
      (null, java.lang.Long.valueOf(20L)),
      (java.lang.Long.valueOf(-1L), java.lang.Long.valueOf(20L)),
      (java.lang.Long.valueOf(0L), null),
      (java.lang.Long.valueOf(0L), java.lang.Long.valueOf(19L)),
      (java.lang.Long.valueOf(0L), java.lang.Long.valueOf(Integer.MAX_VALUE.toLong + 1L)),
      (java.lang.Long.valueOf(Long.MaxValue - 10L), java.lang.Long.valueOf(20L)))

    invalidRanges.foreach { case (offset, size) =>
      assertThrows[IllegalArgumentException] {
        IcebergDeletionVector.read(inputFile, offset, size, 0L)
      }
    }
    assert(inputFile.openCount == 0)
  }
}
