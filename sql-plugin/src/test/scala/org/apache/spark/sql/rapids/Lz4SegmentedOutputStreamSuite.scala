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
package org.apache.spark.sql.rapids

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, OutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.nvidia.spark.rapids.{Lz4SegmentedOutputStream, SegmentedByteArrayOutputStream}
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory}
import net.jpountz.xxhash.XXHashFactory
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigEntry, IO_COMPRESSION_LZ4_BLOCKSIZE}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.ShuffleBlockId

class Lz4SegmentedOutputStreamSuite extends AnyFunSuite {
  private val compressor = LZ4Factory.fastestInstance().fastCompressor()
  private val block = ShuffleBlockId(1, 2, 3)

  private def checksum = XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum()

  private def bytes(out: SegmentedByteArrayOutputStream): Array[Byte] = {
    val result = new ByteArrayOutputStream()
    out.writeTo(result)
    result.toByteArray
  }

  private def decode(manager: SerializerManager, encoded: Array[Byte]): Array[Byte] = {
    val in = manager.wrapStream(block, new ByteArrayInputStream(encoded))
    val result = new ByteArrayOutputStream()
    val buffer = new Array[Byte](777)
    try {
      var size = in.read(buffer)
      while (size >= 0) {
        result.write(buffer, 0, size)
        size = in.read(buffer)
      }
    } finally {
      in.close()
    }
    result.toByteArray
  }

  private def writeMixed(out: OutputStream, input: Array[Byte]): Unit = {
    val prefix = math.min(17, input.length)
    input.take(prefix).foreach(b => out.write(b.toInt))
    out.flush() // Must not finish a partial LZ4 block (Spark uses syncFlush=false).
    out.write(input, prefix, input.length - prefix)
    out.close()
  }

  for ((blockSize, segmentSize) <- Seq((64, 128), (32768, 131072), (65536, 131072))) {
    test(s"Spark wire bytes and decoding match with block=$blockSize segment=$segmentSize") {
      val conf = new SparkConf(false).set("spark.io.compression.codec", "lz4")
        .set("spark.io.compression.lz4.blockSize", blockSize.toString)
      val manager = new SerializerManager(new JavaSerializer(conf), conf)
      val input = new Array[Byte](blockSize * 7 + 19)
      new Random(197).nextBytes(input)
      java.util.Arrays.fill(input, 0, blockSize, 0.toByte)
      java.util.Arrays.fill(input, blockSize * 3, blockSize * 4, 7.toByte)
      val factory = new Lz4SegmentedOutputStream.Factory(blockSize, compressor)
      for (length <- Seq(0, 1, blockSize - 1, blockSize, blockSize + 1, input.length)) {
        val payload = input.take(length)
        val expected = new ByteArrayOutputStream()
        writeMixed(manager.wrapStream(block, expected), payload)
        val result = new SegmentedByteArrayOutputStream(segmentSize)
        val direct = factory.create(result, checksum)
        writeMixed(direct, payload)
        direct.close()
        intercept[IOException](direct.write(1))
        assert(bytes(result).sameElements(expected.toByteArray), s"length=$length")
        assert(decode(manager, bytes(result)).sameElements(payload))
      }
    }
  }

  private class RecordingCompressor extends LZ4Compressor {
    val destinations = new ArrayBuffer[Array[Byte]]()
    val capacities = new ArrayBuffer[Int]()
    var failure: Option[RuntimeException] = None

    override def compress(src: Array[Byte], srcOffset: Int, length: Int,
        dst: Array[Byte], dstOffset: Int, capacity: Int): Int = {
      failure.foreach(throw _)
      destinations += dst
      capacities += capacity
      compressor.compress(src, srcOffset, length, dst, dstOffset, capacity)
    }

    override def compress(src: ByteBuffer, srcOffset: Int, length: Int,
        dst: ByteBuffer, dstOffset: Int, capacity: Int): Int =
      compressor.compress(src, srcOffset, length, dst, dstOffset, capacity)
  }

  test("full and partial blocks use the configured capacity at the exact segment boundary") {
    val blockSize = 32768
    val bound = compressor.maxCompressedLength(blockSize)
    val minimumAvailable = 21 + bound
    val recording = new RecordingCompressor
    val factory = new Lz4SegmentedOutputStream.Factory(blockSize, recording)
    assert(!factory.supports(minimumAvailable - 1))
    assert(factory.supports(minimumAvailable))
    intercept[IllegalArgumentException] {
      factory.create(new SegmentedByteArrayOutputStream(minimumAvailable - 1), checksum)
    }
    val conf = new SparkConf(false).set("spark.io.compression.codec", "lz4")
    val manager = new SerializerManager(new JavaSerializer(conf), conf)
    val input = new Array[Byte](blockSize * 3 + 1)
    new Random(29).nextBytes(input)
    val result = new SegmentedByteArrayOutputStream(minimumAvailable)
    writeMixed(factory.create(result, checksum), input)
    assert(recording.capacities.toSeq == Seq.fill(4)(bound))
    assert(decode(manager, bytes(result)).sameElements(input))
  }

  test("configured block size is read once and retained across record streams") {
    class CountingSparkConf extends SparkConf(false) {
      var blockSizeReads = 0

      override def get[T](entry: ConfigEntry[T]): T = {
        if (entry.key == IO_COMPRESSION_LZ4_BLOCKSIZE.key) {
          blockSizeReads += 1
        }
        super.get(entry)
      }
    }
    val conf = new CountingSparkConf
    conf.set("spark.io.compression.codec", "lz4")
      .set("spark.io.compression.lz4.blockSize", "64k")
    val manager = new SerializerManager(new JavaSerializer(conf), conf)
    val input = new Array[Byte](65536 * 3 + 17)
    new Random(43).nextBytes(input)
    val expected = new ByteArrayOutputStream()
    writeMixed(manager.wrapStream(block, expected), input)
    conf.blockSizeReads = 0
    val compression = new ShuffleRecordCompression(conf, manager)
    for (_ <- 0 until 3) {
      val result = new SegmentedByteArrayOutputStream(131072)
      val stream = compression.wrapStream(block, result)
      assert(stream.isInstanceOf[Lz4SegmentedOutputStream])
      writeMixed(stream, input)
      assert(bytes(result).sameElements(expected.toByteArray))
      assert(decode(manager, bytes(result)).sameElements(input))
      assert(conf.blockSizeReads == 1)
      // Spark Core does not support live updates; the writer keeps its captured parameters.
      conf.set("spark.io.compression.lz4.blockSize", "16k")
    }
  }

  test("compressor writes into the same arrays transferred to the partial-file consumer") {
    val recording = new RecordingCompressor
    val result = new SegmentedByteArrayOutputStream(131072)
    val direct = new Lz4SegmentedOutputStream(result, 32768, recording, checksum)
    direct.write(new Array[Byte](1024 * 1024 + 3))
    direct.close()
    val owned = result.takeBuffer()
    val arrays = new ArrayBuffer[Array[Byte]]()
    try {
      owned.writeTo(new OutputStream {
        override def write(value: Int): Unit = fail("unexpected byte write")
        override def write(data: Array[Byte], offset: Int, length: Int): Unit = arrays += data
      })
      assert(recording.destinations.nonEmpty)
      assert(recording.destinations.forall(dst => arrays.exists(_ eq dst)))
    } finally {
      owned.close()
    }
  }

  test("failed compression aborts its output reservation without publishing partial bytes") {
    val recording = new RecordingCompressor
    val failure = new IllegalStateException("injected compressor failure")
    recording.failure = Some(failure)
    val result = new SegmentedByteArrayOutputStream(131072)
    val direct = new Lz4SegmentedOutputStream(result, 32768, recording, checksum)
    assert(intercept[IllegalStateException](direct.write(new Array[Byte](32768))) eq failure)
    assert(result.getCount == 0 && result.getCapacity == 0)
    val owned = result.takeBuffer() // No outstanding reservation survives the failure.
    owned.close()
  }

  for ((codec, encrypted, compress, segmentSize, optimized) <- Seq(
      ("lz4", false, true, 131072, true),
      ("LZ4", false, true, 131072, true),
      ("org.apache.spark.io.LZ4CompressionCodec", false, true, 131072, true),
      ("lz4", true, true, 131072, false),
      ("lz4", false, false, 131072, false),
      ("lz4", false, true, 8192, false),
      ("lzf", false, true, 131072, false),
      ("snappy", false, true, 131072, false),
      ("zstd", false, true, 131072, false))) {
    test(s"selection and readback: $codec encryption=$encrypted " +
        s"compression=$compress segment=$segmentSize") {
      val conf = new SparkConf(false).set("spark.io.compression.codec", codec)
        .set("spark.shuffle.compress", compress.toString)
      val key = if (encrypted) Some(Array.tabulate[Byte](16)(_.toByte)) else None
      val manager = new SerializerManager(new JavaSerializer(conf), conf, key)
      val result = new SegmentedByteArrayOutputStream(segmentSize)
      val stream = new ShuffleRecordCompression(conf, manager).wrapStream(block, result)
      assert(stream.isInstanceOf[Lz4SegmentedOutputStream] == optimized)
      val input = new Array[Byte](131072 + 39)
      new Random(83).nextBytes(input)
      writeMixed(stream, input)
      assert(decode(manager, bytes(result)).sameElements(input))
    }
  }

  test("ordinary output streams retain the Spark compression path") {
    val conf = new SparkConf(false).set("spark.io.compression.codec", "lz4")
    val manager = new SerializerManager(new JavaSerializer(conf), conf)
    val result = new ByteArrayOutputStream()
    val stream = new ShuffleRecordCompression(conf, manager).wrapStream(block, result)
    assert(!stream.isInstanceOf[Lz4SegmentedOutputStream])
    val input = Array[Byte](1, 2, 3)
    writeMixed(stream, input)
    assert(decode(manager, result.toByteArray).sameElements(input))
  }
}
