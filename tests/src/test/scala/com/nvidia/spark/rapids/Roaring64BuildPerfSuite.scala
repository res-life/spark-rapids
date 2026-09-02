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
 * See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.nvidia.spark.rapids

import java.io.{ByteArrayOutputStream, DataOutputStream}

import ai.rapids.cudf.{ColumnVector => CudfColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RoaringBitmap
import org.apache.iceberg.shaded.org.roaringbitmap.longlong.Roaring64NavigableMap
import org.scalatest.funsuite.AnyFunSuite

/**
 * Opt-in comparison of the Java CPU and JNI GPU Roaring64 build pipelines.
 *
 * Both paths start with the same device column and finish with a portable bitmap in host memory.
 * The CPU path copies all positions to the host, adds them to a Java bitmap one row at a time,
 * optimizes it, and serializes it. The GPU path builds and serializes the bitmap on the GPU and
 * copies only the serialized result to host memory.
 *
 * Usage:
 * {{
 *   mvn package -pl tests -am -Dbuildver=350 \
 *     -DwildcardSuites=com.nvidia.spark.rapids.Roaring64BuildPerfSuite \
 *     -DargLine="-DenableRoaring64Perf=true \
 *       -Droaring64PerfRows=10000,1000000,10000000 \
 *       -Droaring64PerfStrides=1,17 \
 *       -Droaring64PerfWarmupRounds=1 -Droaring64PerfMeasuredRounds=5"
 * }}
 */
class Roaring64BuildPerfSuite extends AnyFunSuite {

  private case class PerfCase(numRows: Int, stride: Long) {
    val name: String = s"rows_${numRows}_stride_$stride"
  }

  private case class PerfResult(
      elapsedNanos: Long,
      cardinality: Long,
      serializedBytes: Long)

  private val enablePerfTest = java.lang.Boolean.getBoolean("enableRoaring64Perf")
  private val warmupRounds = java.lang.Integer.getInteger(
    "roaring64PerfWarmupRounds", 1).intValue()
  private val measuredRounds = java.lang.Integer.getInteger(
    "roaring64PerfMeasuredRounds", 5).intValue()

  private def parsePositiveLongs(property: String, defaults: Seq[Long]): Seq[Long] = {
    Option(System.getProperty(property)).map { configured =>
      configured.split(',').toSeq.map(_.trim).filter(_.nonEmpty).map(_.toLong)
    }.getOrElse(defaults)
  }

  private def time(body: => (Long, Long)): PerfResult = {
    val start = System.nanoTime()
    val (cardinality, serializedBytes) = body
    PerfResult(System.nanoTime() - start, cardinality, serializedBytes)
  }

  private def javaCpuBuild(positions: CudfColumnVector): PerfResult = time {
    withResource(positions.copyToHost()) { hostPositions =>
      val bitmap = new Roaring64NavigableMap()
      var rowIndex = 0
      while (rowIndex < hostPositions.getRowCount) {
        bitmap.addLong(hostPositions.getLong(rowIndex))
        rowIndex += 1
      }
      bitmap.runOptimize()

      val byteStream = new ByteArrayOutputStream()
      withResource(new DataOutputStream(byteStream)) { output =>
        bitmap.serializePortable(output)
        output.flush()
      }
      (bitmap.getLongCardinality, byteStream.size().toLong)
    }
  }

  private def gpuBuild(positions: CudfColumnVector): PerfResult = time {
    withResource(RoaringBitmap.buildAndSerialize64(positions)) { bitmap =>
      (bitmap.getCardinality, bitmap.getSerializedSizeInBytes)
    }
  }

  private def rowsPerSecond(numRows: Int, elapsedNanos: Double): Double = {
    numRows.toDouble * 1000000000.0 / elapsedNanos
  }

  private def median(sortedNanos: Seq[Long]): Double = {
    val middle = sortedNanos.length / 2
    if (sortedNanos.length % 2 == 0) {
      (sortedNanos(middle - 1).toDouble + sortedNanos(middle).toDouble) / 2.0
    } else {
      sortedNanos(middle).toDouble
    }
  }

  private def verifyResults(perfCase: PerfCase, cpu: PerfResult, gpu: PerfResult): Unit = {
    assert(cpu.cardinality === perfCase.numRows.toLong)
    assert(gpu.cardinality === perfCase.numRows.toLong)
    assert(gpu.serializedBytes === cpu.serializedBytes)
  }

  private def reportRun(
      perfCase: PerfCase,
      phase: String,
      round: Int,
      implementation: String,
      result: PerfResult): Unit = {
    val elapsedMs = result.elapsedNanos.toDouble / 1000000.0
    val throughput = rowsPerSecond(perfCase.numRows, result.elapsedNanos.toDouble)
    ConsoleOutput.writeLine(f"ROARING64_PERF_RUN,case=${perfCase.name},phase=$phase," +
      f"round=$round,impl=$implementation,rows=${perfCase.numRows}," +
      f"stride=${perfCase.stride},elapsed_ms=$elapsedMs%.3f," +
      f"rows_per_sec=$throughput%.3f,serialized_bytes=${result.serializedBytes}," +
      f"cardinality=${result.cardinality}")
  }

  private def runRound(
      positions: CudfColumnVector,
      perfCase: PerfCase,
      phase: String,
      round: Int): (PerfResult, PerfResult) = {
    val (cpu, gpu) = if (round % 2 == 1) {
      (javaCpuBuild(positions), gpuBuild(positions))
    } else {
      val gpuResult = gpuBuild(positions)
      (javaCpuBuild(positions), gpuResult)
    }
    verifyResults(perfCase, cpu, gpu)
    reportRun(perfCase, phase, round, "java_cpu", cpu)
    reportRun(perfCase, phase, round, "gpu_jni", gpu)
    (cpu, gpu)
  }

  private def reportSummary(
      perfCase: PerfCase,
      cpuResults: Seq[PerfResult],
      gpuResults: Seq[PerfResult]): Unit = {
    val cpuMedianNanos = median(cpuResults.map(_.elapsedNanos).sorted)
    val gpuMedianNanos = median(gpuResults.map(_.elapsedNanos).sorted)
    val cpuMedianMs = cpuMedianNanos / 1000000.0
    val gpuMedianMs = gpuMedianNanos / 1000000.0
    val cpuThroughput = rowsPerSecond(perfCase.numRows, cpuMedianNanos)
    val gpuThroughput = rowsPerSecond(perfCase.numRows, gpuMedianNanos)
    val speedup = cpuMedianNanos / gpuMedianNanos
    ConsoleOutput.writeLine(f"ROARING64_PERF_SUMMARY,case=${perfCase.name}," +
      f"rows=${perfCase.numRows},stride=${perfCase.stride}," +
      f"measured_rounds=$measuredRounds,java_cpu_median_ms=$cpuMedianMs%.3f," +
      f"gpu_jni_median_ms=$gpuMedianMs%.3f,java_cpu_rows_per_sec=$cpuThroughput%.3f," +
      f"gpu_jni_rows_per_sec=$gpuThroughput%.3f,speedup=$speedup%.3f," +
      f"serialized_bytes=${gpuResults.head.serializedBytes}")
  }

  private def runCase(perfCase: PerfCase): Unit = {
    val values = new Array[Long](perfCase.numRows)
    var index = 0
    while (index < values.length) {
      values(index) = index.toLong * perfCase.stride
      index += 1
    }

    withResource(CudfColumnVector.fromLongs(values: _*)) { positions =>
      (1 to warmupRounds).foreach { round =>
        runRound(positions, perfCase, "warmup", round)
      }
      val measured = (1 to measuredRounds).map { round =>
        runRound(positions, perfCase, "measured", round)
      }
      reportSummary(perfCase, measured.map(_._1), measured.map(_._2))
    }
  }

  test("Roaring64 Java CPU and GPU build performance") {
    assume(enablePerfTest,
      "set -DenableRoaring64Perf=true to run the Roaring64 build benchmark")
    assert(warmupRounds >= 0, "roaring64PerfWarmupRounds must not be negative")
    assert(measuredRounds > 0, "roaring64PerfMeasuredRounds must be positive")

    val rowCounts = parsePositiveLongs(
      "roaring64PerfRows", Seq(10000L, 1000000L, 10000000L))
    val strides = parsePositiveLongs("roaring64PerfStrides", Seq(1L, 17L))
    assert(rowCounts.nonEmpty && rowCounts.forall(rows => rows > 0 && rows <= Int.MaxValue),
      "roaring64PerfRows must contain positive Int-sized values")
    assert(strides.nonEmpty && strides.forall(_ > 0),
      "roaring64PerfStrides must contain positive values")

    val cases = for {
      numRows <- rowCounts
      stride <- strides
    } yield {
      assert((numRows - 1) * stride <= RoaringBitmap.MAX_POSITION,
        s"maximum position for rows=$numRows,stride=$stride exceeds Roaring64 limit")
      PerfCase(numRows.toInt, stride)
    }
    cases.foreach(runCase)
  }
}
