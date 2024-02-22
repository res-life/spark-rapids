/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.File
import java.util.UUID

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.timezone.{StringGenFunc, TimeZonePerfUtils}
import org.apache.spark.sql.types._

/**
 * A simple test performance framework for get-json-object.
 * Usage:
 *
 * argLine="-DenableGetJsonObjectPerf=true" \
 * mvn test -Dbuildver=311 -DwildcardSuites=com.nvidia.spark.rapids.GetJsonObjectPerfSuite
 * Note:
 * Generate a Parquet file with 1 columns:
 *     - c_str_of_json: JSON string column
 *       The c_str_of_json column is high duplicated.
 *       The generated file is highly compressed since we expect both CPU and GPU can scan quickly.
 *       When testing operators, we need to add in a max/count aggregator to reduce the result data.
 */
class GetJsonObjectPerfSuite extends SparkQueryCompareTestSuite with BeforeAndAfterAll {
  private val enablePerfTest = java.lang.Boolean.getBoolean("enableGetJsonObjectPerf")

  // rows for perf test
  private val numRows: Long = 1024L * 1024L * 10L

  private val path = "/tmp/tmp_GetJsonObjectPerfSuite" + UUID.randomUUID();

  /**
   * Create a Parquet file to test
   */
  override def beforeAll(): Unit = {
    withCpuSparkSession(
      spark => createDF(spark).write.mode("overwrite").parquet(path))
  }

  override def afterAll(): Unit = {
    org.apache.spark.sql.FileUtils.deleteRecursively(new File(path))
  }

  private val jsonStrings = Array(
    "{ 'k1' : { 'k2': { 'k3': { 'k4': { 'k5': { 'k6': { 'k7': { 'k8': { 'k9': 'string\\n\\r' " +
        "}}}}}}}}} ",
    "{ 'k1' : { 'k2': { 'k3': { 'k4': { 'k5': { 'k6': { 'k7': { 'k8': { 'k9': null " +
        "}}}}}}}}} "
  )

  def createDF(spark: SparkSession): DataFrame = {
    val id = col("id")
    val columns = Array[Column](
      TimeZonePerfUtils.createColumn(id, StringType, StringGenFunc(jsonStrings))
          .alias("c_str_of_json")
    )
    val range = spark.range(numRows)
    range.select(columns: _*)
  }

  /**
   * Run 6 rounds for both Cpu and Gpu,
   * but only print the elapsed times for the last 5 rounds.
   */
  def runAndRecordTime(
      testName: String,
      func: (SparkSession) => DataFrame,
      conf: SparkConf = new SparkConf()): Any = {

    if (!enablePerfTest) {
      // by default skip perf test
      return None
    }
    println(s"test,type,used MS")

    // run 6 rounds, but ignore the first round.
    val elapses = (1 to 6).map { i =>
      // run on Cpu
      val startOnCpu = System.nanoTime()
      withCpuSparkSession(
        spark => func(spark).collect())
      val endOnCpu = System.nanoTime()
      val elapseOnCpuMS = (endOnCpu - startOnCpu) / 1000000L
      if (i != 1) {
        println(s"$testName,Cpu,$elapseOnCpuMS")
      }

      // run on Gpu
      val startOnGpu = System.nanoTime()
      withGpuSparkSession(
        spark => func(spark).collect(),
        conf.set("spark.rapids.sql.expression.GetJsonObject", "true"))
      val endOnGpu = System.nanoTime()
      val elapseOnGpuMS = (endOnGpu - startOnGpu) / 1000000L
      if (i != 1) {
        println(s"$testName,Gpu,$elapseOnGpuMS")
        (elapseOnCpuMS, elapseOnGpuMS)
      } else {
        (0L, 0L) // skip the first round
      }
    }
    val meanCpu = elapses.map(_._1).sum / 5.0
    val meanGpu = elapses.map(_._2).sum / 5.0
    val speedup = meanCpu.toDouble / meanGpu.toDouble
    println(f"$testName: mean cpu time: $meanCpu%.2f ms, " +
        f"mean gpu time: $meanGpu%.2f ms, speedup: $speedup%.2f x")

  }

  test("test get-json-object") {
    assume(enablePerfTest)

    def perfTest(spark: SparkSession): DataFrame = {
      // use count to reduce the result data
      spark.read.parquet(path).select(functions.count(
        functions.get_json_object(functions.col("c_str_of_json"),
          "$.k1.k2.k3.k4.k5.k6.k7.k8.k9")
      ))
    }

    runAndRecordTime("get-json-object", perfTest)
  }
}
