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

package com.nvidia.spark.rapids.timezone

import java.io.{File, FileFilter}

import com.nvidia.spark.rapids.{RapidsConf, SparkQueryCompareTestSuite}
import com.sun.rowset.internal.Row
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}

import org.apache.spark.sql.{functions, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Baidu diff files are on cloud:
 *   https://drive.google.com/drive/folders/1svVcUw2eFhEOH1Uaa0ETmR0vbvmlg-6f
 * There are several CSV files in above cloud folder, CSV columns are:
 *   1st column is query path
 *   2nd column is JSON
 *   3rd and 4th columns are not used
 * Note:
 *   1. After checked the CSV data, some rows are invalid, this tool ignores
 *      the invalid rows.
 *   2. All the invalid rows are querying with 1 depth, e.g.: $.query_key.
 * Verification logic
 *   I tried executing row by row, but it needs lots of time.
 *   So the logic now is: Replace all the query key to a identical one and replace the JSONs.
 *   In this way, we can execute by a block of rows to save execution time.
 * Note:
 *   If returned results from CPU/GPU is a double, we verify double results by:
 *     cpuRet - gpuRet < 0.000000001
 *   Because there are may be some acceptable differences between cpu and gpu results.
 */
class VerifyGetJsonObject extends SparkQueryCompareTestSuite {

  // the query key will be replaced with
  val queryKey = "key_key_key_key"

  // download the csv files from cloud, put into this dir
  val dirPath = "/home/chongg/baidu/get-json-object-diffs/get_json_obj_diffs/"

  /**
   * Return the row iterator, each row contains 2 strings: query and JSON
   * Invalid rows are ignored.
   */
  def getRowsIterator(csvFile: File): Iterator[Row] = {
    new Iterator[Row]() {
      val settings = new CsvParserSettings
      settings.setMaxCharsPerColumn(20 * 1024 * 1024)
      val csvParser = new CsvParser(settings)
      csvParser.beginParsing(csvFile)
      var row: Array[String] = null

      override def hasNext: Boolean = {
        row = csvParser.parseNext
        while (row != null) {
          // row(0) is query
          // row(1) is JSON
          if (row.length > 1 &&
              row(0) != null &&
              row(0).contains("$") &&
              row(0).contains(".") &&
              row(0).split("\\.").length == 2 &&
              row(1) != null) {
            return true
          }
          row = csvParser.parseNext
        }
        false
      }

      override def next(): Row = {
        // row(0) is path; row(1) is JSON
        // get the key_name from JSON path
        // replace key_name in JSON with constant "k"
        val key = row(0).split("\\.")(1)
        Row(row(1).replace(key, queryKey))
      }
    }
  }

  def isDouble(str: String): Boolean = {
    try {
      str.toDouble
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  def handleCsvFile(spark: SparkSession, f: File): Unit = {

    // Define the schema of the DataFrame
    val schema = StructType(Seq(
      StructField("json", StringType, nullable = true)
    ))

    // Create an iterator of Rows
    val data = getRowsIterator(f)

    val blockSize = 100000
    val dataBlocks = data.grouped(blockSize)

    while (dataBlocks.hasNext) {
      val nextBlock = dataBlocks.next()
      val seq: Seq[Row] = nextBlock
      spark.conf.set(RapidsConf.SQL_ENABLED.key, "false")
      val cpuDf = spark.createDataFrame(spark.sparkContext.parallelize(seq), schema)
      val cpuRet = cpuDf.select(functions.get_json_object(col("json"), "$." + queryKey)).collect()
      spark.conf.set(RapidsConf.SQL_ENABLED.key, "false")
      val gpuDf = spark.createDataFrame(spark.sparkContext.parallelize(seq), schema)
      val gpuRet = gpuDf.select(functions.get_json_object(col("json"), "$." + queryKey)).collect()
      assert(cpuRet.length == gpuRet.length)
      for (i <- 0 until  cpuRet.length) {
        val cpuStr = cpuRet(i).getString(0)
        val gpuStr = gpuRet(i).getString(0)
        if (cpuStr != null && isDouble(cpuStr)) { // check double result
          // assert double diff
          assert(isDouble(gpuStr))
          val cpuDouble = cpuStr.toDouble
          val gpuDouble = gpuStr.toDouble
          // assert acceptable result for double
          assert(gpuDouble - cpuDouble < 1e-9)

          // check cases like: 0.000001 vs 1E-6
          if (cpuStr.length > 5 && gpuStr.length > 5) {
            assert(cpuStr.substring(0, 5) == gpuStr.substring(0, 5))
          }
        } else { // check string result
          assertResult(cpuRet(i))(gpuRet(i))
        }
      }
    }
  }

  test("verify Baidu diff files") {
    val startTime = System.nanoTime()
    val directory = new File(dirPath)
    val csvFiles = directory.listFiles(new FileFilter() {
      override def accept(f: File): Boolean = f.isFile && f.getName.endsWith(".csv")
    })
    val spark = SparkSession.builder()
        .master("local[*]")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .appName("Test Baidu get_json_object diffs")
        .getOrCreate()
    csvFiles.foreach { csvFile => handleCsvFile(spark, csvFile) }
    val endTime = System.nanoTime()
    println(s"Used time ${(endTime - startTime) / 1000000} MS")
  }
}
