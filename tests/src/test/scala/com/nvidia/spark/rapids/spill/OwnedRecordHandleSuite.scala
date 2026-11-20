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
package com.nvidia.spark.rapids.spill

import java.io.IOException
import java.nio.file.Files

import com.nvidia.spark.rapids.{RapidsConf, SegmentedByteArrayOutputStream}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class OwnedRecordHandleSuite extends AnyFunSuite with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    super.beforeEach()
    SpillFramework.initialize(new RapidsConf(Map.empty[String, String]))
    assert(PartialFileHeapBudget.bytesReserved == 0)
  }
  override def afterEach(): Unit = {
    try {
      SpillFramework.shutdown()
      assert(PartialFileHeapBudget.bytesReserved == 0)
    } finally {
      super.afterEach()
    }
  }
  private def record(data: Array[Byte], segment: Int = 7): SegmentedByteArrayOutputStream = {
    val result = new SegmentedByteArrayOutputStream(segment)
    result.write(data)
    result.close()
    result
  }
  private def create(file: java.io.File, max: Long = 1048576, budget: Long = 1048576) = {
    SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 32 * 1024 * 1024, maxBufferSize = max,
      memoryThreshold = 1.0, spillFile = file, retainRecords = true, heapLimit = budget,
      capacityHintProvider = Some((_, _) => fail("owned records cannot predict capacity")))
  }

  for (segment <- Seq(1, 7, 8192, 131072)) {
    test(s"$segment-byte records read across tails and spill with exact capacity accounting") {
      val file = Files.createTempFile("owned-record", ".bin").toFile
      val a = Array.tabulate[Byte](segment + 3)(_.toByte)
      val b = Array.tabulate[Byte](segment * 2 + 1)(i => (i * 17).toByte)
      val expected = a ++ b
      withResource(create(file)) { handle =>
        assert(handle.getAllocatedCapacity == 0)
        val first = record(a, segment)
        val second = record(b, segment)
        val capacity = first.getCapacity + second.getCapacity
        handle.writeRecord(first)
        handle.writeRecord(record(Array.emptyByteArray, segment))
        handle.writeRecord(second)
        assert(handle.getAllocatedCapacity == capacity)
        assert(PartialFileHeapBudget.bytesReserved == capacity)
        assert(SpillFramework.stores.hostStore.totalSize == capacity)
        assert(handle.getExpansionCopiedBytes == 0)
        handle.finishWrite()
        for (offset <- Seq(0, a.length - 2, a.length, expected.length - 1)) {
          val actual = new Array[Byte](expected.length - offset)
          assert(handle.readAt(offset, actual, 0, actual.length) == actual.length)
          assert(actual.sameElements(expected.drop(offset)))
        }
        val prefix = new Array[Byte](a.length - 1)
        assert(handle.read(prefix, 0, prefix.length) == prefix.length)
        assert(prefix.sameElements(expected.take(prefix.length)))
        assert(handle.spillForCommit() == expected.length)
        assert(PartialFileHeapBudget.bytesReserved == 0)
        assert(SpillFramework.stores.hostStore.totalSize == 0)
        assert(Files.readAllBytes(file.toPath).sameElements(expected))
        val suffix = new Array[Byte](expected.length - prefix.length)
        assert(handle.read(suffix, 0, suffix.length) == suffix.length)
        assert(suffix.sameElements(expected.drop(prefix.length)))
      }
      assert(!file.exists())
    }
  }

  test("executor budget exhaustion spills without waiting on another unfinished writer") {
    withResource(create(Files.createTempFile("owned-first", ".bin").toFile, budget = 14)) { a =>
      withResource(create(Files.createTempFile("owned-second", ".bin").toFile, budget = 14)) { b =>
        a.writeRecord(record(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)))
        assert(PartialFileHeapBudget.bytesReserved == 14)
        b.writeRecord(record(Array[Byte](42)))
        assert(b.isSpilled)
        b.finishWrite()
        val actual = new Array[Byte](1)
        assert(b.read(actual, 0, 1) == 1 && actual(0) == 42)
        assert(PartialFileHeapBudget.bytesReserved == 14)
      }
    }
  }

  for (ordinary <- Seq(false, true)) {
    test(s"capacity fallback preserves order, ordinary write=$ordinary") {
      withResource(create(Files.createTempFile("owned-limit", ".bin").toFile, max = 14)) { h =>
        val bytes = Array.tabulate[Byte](9)(_.toByte)
        h.writeRecord(record(bytes))
        if (ordinary) h.write(42) else h.writeRecord(record(Array[Byte](42)))
        assert(h.isSpilled)
        assert(PartialFileHeapBudget.bytesReserved == 0)
        h.finishWrite()
        val actual = new Array[Byte](10)
        assert(h.read(actual, 0, 10) == 10)
        assert(actual.sameElements(bytes :+ 42.toByte))
      }
    }
  }

  test("failed spill retains prior records and releases the rejected incoming record") {
    val path = Files.createTempDirectory("owned-spill-failure")
    withResource(create(path.toFile, max = 7)) { h =>
      h.writeRecord(record(Array[Byte](1, 2, 3)))
      intercept[IOException](h.writeRecord(record(Array[Byte](4))))
      assert(!h.isSpilled)
      assert(PartialFileHeapBudget.bytesReserved == 7)
      assert(SpillFramework.stores.hostStore.totalSize == 7)
      Files.delete(path)
      h.writeRecord(record(Array[Byte](4)))
      h.finishWrite()
      val actual = new Array[Byte](4)
      assert(h.read(actual, 0, 4) == 4)
      assert(actual.sameElements(Array[Byte](1, 2, 3, 4)))
    }
  }

  test("read lease keeps transferred records alive until the final consumer closes") {
    val h = create(Files.createTempFile("owned-lease", ".bin").toFile)
    h.writeRecord(record(Array[Byte](42)))
    h.finishWrite()
    h.acquireRead()
    try {
      h.close()
      assert(PartialFileHeapBudget.bytesReserved == 7)
      val actual = new Array[Byte](1)
      assert(h.readAt(0, actual, 0, 1) == 1 && actual(0) == 42)
      intercept[IllegalStateException](h.writeRecord(record(Array[Byte](1))))
    } finally {
      h.releaseRead()
      h.close()
    }
    assert(PartialFileHeapBudget.bytesReserved == 0)
  }
}
