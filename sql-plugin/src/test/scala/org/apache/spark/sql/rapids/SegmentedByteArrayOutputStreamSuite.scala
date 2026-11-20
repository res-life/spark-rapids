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

import scala.util.Random

import com.nvidia.spark.rapids.{RapidsConf, SegmentedByteArrayOutputStream}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.ShuffleBlockId

class SegmentedByteArrayOutputStreamSuite extends AnyFunSuite {
  private val segmentKey = "spark.rapids.shuffle.multiThreaded.writer.buffer.segmentSize"

  test("segment configuration defaults to 1 MiB and validates byte sizes") {
    assert(new RapidsConf(Map.empty[String, String]).shuffleMultiThreadedWriterBufferSegmentSize ==
      1024 * 1024)
    for ((value, expected) <- Seq("0" -> 0, "1m" -> 1048576, "64k" -> 65536)) {
      assert(new RapidsConf(Map(segmentKey -> value)).shuffleMultiThreadedWriterBufferSegmentSize ==
        expected)
    }
    for (value <- Seq("-1", "2g")) {
      intercept[IllegalArgumentException] {
        new RapidsConf(Map(segmentKey -> value)).shuffleMultiThreadedWriterBufferSegmentSize
      }
    }
  }

  for (segmentSize <- Seq(1, 31, 1024, 1024 * 1024)) {
    test(s"mixed writes preserve all bytes across $segmentSize-byte segment boundaries") {
      val out = new SegmentedByteArrayOutputStream(segmentSize)
      val expected = new ByteArrayOutputStream()
      val random = new Random(723)
      val bytes = new Array[Byte](math.max(100, segmentSize * 3 + 17))
      random.nextBytes(bytes)
      assert(out.getCapacity == 0)
      out.write(bytes, 0, 0)
      assert(out.getCapacity == 0)
      var offset = 0
      while (offset < bytes.length) {
        if (random.nextBoolean()) {
          out.write(bytes(offset).toInt)
          expected.write(bytes(offset).toInt)
          offset += 1
        } else {
          val length = math.min(bytes.length - offset, random.nextInt(8192) + 1)
          out.write(bytes, offset, length)
          expected.write(bytes, offset, length)
          offset += length
        }
      }
      out.close()
      assert(out.getCount == bytes.length)
      assert(out.getCapacity == ((bytes.length.toLong - 1) / segmentSize + 1) * segmentSize)
      // Closing a codec closes its underlying stream. The merger still needs to read it.
      for (_ <- 0 until 2) {
        val actual = new ByteArrayOutputStream()
        out.writeTo(actual)
        assert(actual.toByteArray.sameElements(expected.toByteArray))
      }
    }
  }

  test("invalid writes preserve data and an exact boundary does not allocate an extra segment") {
    val out = new SegmentedByteArrayOutputStream(4)
    out.write(Array[Byte](1, 2, 3, 4))
    assert(out.getCapacity == 4)
    for ((offset, length) <- Seq((-1, 1), (0, -1), (3, 2), (Int.MaxValue, 1))) {
      intercept[IndexOutOfBoundsException](out.write(new Array[Byte](4), offset, length))
    }
    intercept[NullPointerException](out.write(null, 0, 0))
    intercept[IllegalArgumentException](out.writeTo(out))
    assert(out.getCount == 4)
    assert(out.getCapacity == 4)
    out.write(5)
    assert(out.getCount == 5)
    assert(out.getCapacity == 8)
    for (size <- Seq(0, -1, Int.MaxValue)) {
      intercept[IllegalArgumentException](new SegmentedByteArrayOutputStream(size))
    }
  }

  test("writeTo propagates failure without consuming data or closing the destination") {
    val out = new SegmentedByteArrayOutputStream(4)
    val bytes = Array.tabulate[Byte](13)(_.toByte)
    out.write(bytes)
    val failure = new IOException("injected destination failure")
    var closed = false
    val broken = new OutputStream {
      override def write(value: Int): Unit = throw failure
      override def close(): Unit = { closed = true }
    }
    assert(intercept[IOException](out.writeTo(broken)) eq failure)
    assert(!closed)
    val actual = new ByteArrayOutputStream()
    out.writeTo(actual)
    assert(actual.toByteArray.sameElements(bytes))
  }

  for (codec <- Seq("lz4", "lzf", "snappy", "zstd"); encrypted <- Seq(false, true)) {
    test(s"$codec shuffle roundtrip across segments with encryption=$encrypted") {
      val conf = new SparkConf(false).set("spark.shuffle.compress", "true")
        .set("spark.io.compression.codec", codec)
      val key = if (encrypted) Some(Array.tabulate[Byte](16)(_.toByte)) else None
      val manager = new SerializerManager(new JavaSerializer(conf), conf, key)
      val block = ShuffleBlockId(1, 2, 3)
      val bytes = new Array[Byte](3 * 1024 * 1024 + 103)
      new Random(19).nextBytes(bytes)
      val buffer = new SegmentedByteArrayOutputStream(1024 * 1024)
      val compressed = manager.wrapStream(block, buffer)
      try {
        compressed.write(bytes)
      } finally {
        compressed.close()
      }
      val serialized = new ByteArrayOutputStream()
      buffer.writeTo(serialized)
      assert(buffer.getCount == serialized.size())
      assert(buffer.getCapacity >= buffer.getCount)
      val in = manager.wrapStream(block, new ByteArrayInputStream(serialized.toByteArray))
      val restored = new ByteArrayOutputStream()
      val scratch = new Array[Byte](8192)
      try {
        var length = in.read(scratch)
        while (length >= 0) {
          restored.write(scratch, 0, length)
          length = in.read(scratch)
        }
      } finally {
        in.close()
      }
      assert(restored.toByteArray.sameElements(bytes))
    }
  }

  test("ownership transfer retains the exact arrays and invalidates the producer") {
    val stream = new SegmentedByteArrayOutputStream(7)
    val bytes = Array.tabulate[Byte](33)(_.toByte)
    stream.write(bytes)
    val before = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    val after = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    def capture(target: scala.collection.mutable.ArrayBuffer[Array[Byte]]) = new OutputStream {
      override def write(value: Int): Unit = fail("unexpected single-byte write")
      override def write(data: Array[Byte], start: Int, length: Int): Unit = target += data
    }
    stream.writeTo(capture(before))
    stream.close() // codec close leaves the payload available
    val owned = stream.takeBuffer()
    try {
      owned.writeTo(capture(after))
      assert(before.size == after.size)
      assert(before.zip(after).forall { case (a, b) => a eq b })
      assert(owned.getCount == 33 && owned.getCapacity == 35)
      intercept[IllegalStateException](stream.write(1))
      intercept[IllegalStateException](stream.write(new Array[Byte](0)))
      intercept[IllegalStateException](stream.writeTo(new ByteArrayOutputStream()))
      intercept[IllegalStateException](stream.takeBuffer())
      stream.close()
      val actual = new Array[Byte](29)
      owned.read(3, actual, 1, 27)
      assert(actual.slice(1, 28).sameElements(bytes.slice(3, 30)))
      intercept[IndexOutOfBoundsException](owned.read(32, actual, 0, 2))
    } finally {
      owned.close()
      owned.close()
    }
    intercept[IllegalStateException](owned.read(0, new Array[Byte](1), 0, 1))
  }

}
