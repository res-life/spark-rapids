/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.timezone

import java.util.UUID

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._
import org.apache.spark.sql.tests.datagen.DBGen

/**
 * Usage:
 *
 * argLine="-DenableCaseWhenPerf=true" \
 * mvn test -Dbuildver=330 -DwildcardSuites=com.nvidia.spark.rapids.timezone.CaseWhenPerfSuite
 * Generate a Parquet file with 10 boolean columns:
 */
class CaseWhenPerfSuite extends SparkQueryCompareTestSuite with BeforeAndAfterAll {
  // perf test is disabled by default since it's a long running time in UT.
//  private val enablePerfTest = java.lang.Boolean.getBoolean("enableCaseWhenPerf")

  // rows for perf test
  private val numRows: Long = 1024L * 1024L * 10L

  private val path = "/tmp/tmp_TimeZonePerfSuite_" + UUID.randomUUID()

  /**
   * Create a Parquet file to test
   */
  override def beforeAll(): Unit = {
    withCpuSparkSession(
      spark => createDF(spark).write.mode("overwrite").parquet(path))
  }

  override def afterAll(): Unit = {
    //    FileUtils.deleteRecursively(new File(path))
  }

  def createDF(spark: SparkSession): DataFrame = {
    val schemaStr =
      """
  struct<
    b01: boolean,
    b02: boolean,
    b03: boolean,
    b04: boolean,
    b05: boolean,
    b06: boolean,
    b07: boolean,
    b08: boolean,
    b09: boolean,
    b10: boolean
  >
"""
    val gen = DBGen()
    val tab = gen.addTable("tab", schemaStr, numRows)
    tab.toDF(spark)
  }

  /**
   * Run 6 rounds for both Cpu and Gpu,
   * but only print the elapsed times for the last 5 rounds.
   */
//  def runAndRecordTime(
//      testName: String,
//      func: (SparkSession, String) => DataFrame,
//      conf: SparkConf = new SparkConf()): Any = {
//
//    println(s"test,type,zone,used MS")
//
//    // run 6 rounds, but ignore the first round.
//    val elapses = (1 to 6).map { i =>
//      // run on Cpu
//      val startOnCpu = System.nanoTime()
//      withCpuSparkSession(
//        spark => func(spark, zoneStr).collect(),
//        // set session time zone
//        conf.set("spark.sql.session.timeZone", zoneStr))
//      val endOnCpu = System.nanoTime()
//      val elapseOnCpuMS = (endOnCpu - startOnCpu) / 1000000L
//      if (i != 1) {
//        println(s"$testName,Cpu,$zoneStr,$elapseOnCpuMS")
//      }
//
//      // run on Gpu
//      val startOnGpu = System.nanoTime()
//      withGpuSparkSession(
//        spark => func(spark, zoneStr).collect(),
//        // set session time zone
//        conf.set("spark.sql.session.timeZone", zoneStr))
//      val endOnGpu = System.nanoTime()
//      val elapseOnGpuMS = (endOnGpu - startOnGpu) / 1000000L
//      if (i != 1) {
//        println(s"$testName,Gpu,$zoneStr,$elapseOnGpuMS")
//        (elapseOnCpuMS, elapseOnGpuMS)
//      } else {
//        (0L, 0L) // skip the first round
//      }
//    }
//    val meanCpu = elapses.map(_._1).sum / 5.0
//    val meanGpu = elapses.map(_._2).sum / 5.0
//    val speedup = meanCpu.toDouble / meanGpu.toDouble
//    println(f"$testName, $zoneStr: mean cpu time: $meanCpu%.2f ms, " +
//        f"mean gpu time: $meanGpu%.2f ms, speedup: $speedup%.2f x")
//  }

  test("test case when perf") {
    //    // cache time zone DB in advance
    //    GpuTimeZoneDB.cacheDatabase()
    //    Thread.sleep(5L)
    //
    //    def perfTest(spark: SparkSession, zone: String): DataFrame = {
    //      spark.read.parquet(path).select(functions.max( // use max to reduce the result data
    //        functions.from_utc_timestamp(functions.col("c_ts"), zone)
    //      ))
    //    }
    //
    //    runAndRecordTime("from_utc_timestamp", perfTest)

    println("success!!!")
  }
}
