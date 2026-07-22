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

package com.nvidia.spark.rapids.timezone

import java.io.File
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.util.{Random, TimeZone}
import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.{GpuOrcTimezoneUtils, RapidsConf, RapidsReaderType,
  SparkQueryCompareTestSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

/**
 * Test suite for ORC reader/writer timezones.
 *
 * Test all combinations of writer/reader for the following timezones:
 *   - `UTC`
 *   - `America/New_York`
 *   - `America/Los_Angeles`
 *   - `Asia/Shanghai`
 *   - `US/Pacific` (alias of `America/Los_Angeles`)
 *   - `PST` (legacy short ID)
 *
 * For each writer/reader timezone pair x 2 datasource versions (v1/v2), the suite:
 *   1. Writes an ORC file on CPU with JVM default timezone set to the writer timezone.
 *   2. Reads it back with JVM default timezone set to each reader timezone.
 *   3. Compares CPU and GPU read results for correctness.
 *
 * Note: reader/writer timezones are controlled by `TimeZone.getDefault`.
 * TimeZone must be set INSIDE the session lambda because resetSparkSessionConf
 * restores spark.sql.session.timeZone to the original value (UTC),
 * which also resets TimeZone.getDefault().
 *
 * Run it manually with:
 *   mvn test -DwildcardSuites=com.nvidia.spark.rapids.OrcTimezoneSuite -Dbuildver=xxx
 *
 * Note: use `orc-tool meta -t orc_file` to view the timezone in each stripe metadata.
 * Each stripe has a timezone in its metadata.
 */
class OrcTimezoneSuite extends SparkQueryCompareTestSuite {

  test("resolve and compare ORC writer timezones") {
    val defaultZone = ZoneId.systemDefault()
    assert(GpuOrcTimezoneUtils.resolveWriterTimezone("") === defaultZone)

    val aliases = Seq("America/Los_Angeles", "US/Pacific", "PST")
      .map(GpuOrcTimezoneUtils.resolveWriterTimezone)
    assert(GpuOrcTimezoneUtils.writerTimezonesShareRules(aliases))
    assert(GpuOrcTimezoneUtils.writerTimezonesShareRules(Seq(
      GpuOrcTimezoneUtils.resolveWriterTimezone("UTC"),
      GpuOrcTimezoneUtils.resolveWriterTimezone("GMT"))))
    assert(!GpuOrcTimezoneUtils.writerTimezonesShareRules(Seq(
      GpuOrcTimezoneUtils.resolveWriterTimezone("UTC"),
      GpuOrcTimezoneUtils.resolveWriterTimezone("America/Los_Angeles"))))

    val error = intercept[IllegalArgumentException] {
      GpuOrcTimezoneUtils.resolveWriterTimezone("Not/AZone")
    }
    assert(error.getMessage.contains("Not/AZone"))
  }

  private val RandomRowCount = 4096L
  // Exact Asia/Shanghai writer=reader reproducer for the ORC epoch borrow correction.
  private val ShanghaiEpochBorrowTsUs = -7713116127L

  // Includes legacy/alias IDs ("US/Pacific", "PST") alongside canonical region IDs to
  // exercise the read path against the kinds of writer-timezone strings ORC footers can
  // actually carry. java.util.TimeZone accepts these even though ZoneId.of rejects them
  // on JDK 21.
  private val timezones = Seq(
    "UTC",
    "America/New_York",
    "America/Los_Angeles",
    "Asia/Shanghai",
    "US/Pacific",
    "PST"
  )

  private val minTs =
    LocalDateTime.of(1970, 1, 2, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) *
      TimeUnit.SECONDS.toMicros(1)
  private val maxTs =
    LocalDateTime.of(9999, 12, 31, 23, 59, 59).toEpochSecond(ZoneOffset.UTC) *
      TimeUnit.SECONDS.toMicros(1) + 999999L

  // 2024 DST transitions for the two canonical DST zones in the test matrix.
  private val DstTransitions = Seq(
    Instant.parse("2024-03-10T07:00:00Z"), // America/New_York spring forward
    Instant.parse("2024-11-03T06:00:00Z"), // America/New_York fall back
    Instant.parse("2024-03-10T10:00:00Z"), // America/Los_Angeles spring forward
    Instant.parse("2024-11-03T09:00:00Z")  // America/Los_Angeles fall back
  )

  private val ExplicitTimestampMicros = {
    val dstBoundaries = DstTransitions.flatMap { transition =>
      val atTransition = TimeUnit.SECONDS.toMicros(transition.getEpochSecond) +
        TimeUnit.NANOSECONDS.toMicros(transition.getNano)
      Seq(atTransition - 1L, atTransition, atTransition + 1L)
    }
    Seq(ShanghaiEpochBorrowTsUs, minTs, maxTs) ++ dstBoundaries
  }

  private def setSessionTimeZone(spark: SparkSession, tzId: String): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone(tzId))
    spark.conf.set("spark.sql.session.timeZone", tzId)
  }

  private def timestampDataFrame(
      spark: SparkSession,
      micros: Seq[Long],
      idOffset: Long = 0L): DataFrame = {
    import spark.implicits._
    val rows = micros.zipWithIndex.map { case (us, i) =>
      val seconds = Math.floorDiv(us, TimeUnit.SECONDS.toMicros(1))
      val microsWithinSecond = Math.floorMod(us, TimeUnit.SECONDS.toMicros(1))
      val ts = Timestamp.from(Instant.ofEpochSecond(seconds, microsWithinSecond * 1000L))
      (idOffset + i, ts)
    }
    rows.toDF("id", "ts")
  }

  private def fileDataFrame(
      spark: SparkSession,
      random: Random,
      idOffset: Long = 0L): DataFrame = {
    val randomMicros = random.longs(RandomRowCount, minTs, maxTs).toArray
    timestampDataFrame(spark, ExplicitTimestampMicros ++ randomMicros, idOffset)
  }

  private val v1SourceLists = Seq("orc", "")

  private def baseConf(v1SourceList: String): SparkConf = {
    new SparkConf()
      .set("spark.sql.sources.useV1SourceList", v1SourceList)
  }

  private def writeFile(spark: SparkSession, outputPath: File, random: Random): Unit = {
    fileDataFrame(spark, random)
      .coalesce(1)
      .write
      .mode("overwrite")
      .orc(outputPath.getCanonicalPath)
  }

  private val timestampSourceTypes = Seq(
    "boolean" -> "true",
    "tinyint" -> "1",
    "smallint" -> "1",
    "int" -> "1593604800",
    "bigint" -> "1593604800",
    "float" -> "1593604800.25",
    "double" -> "1593604800.25")

  for {
    (sourceType, value) <- timestampSourceTypes
    useChunkedReader <- Seq(false, true)
  } {
    test(s"schema evolution from $sourceType to timestamp, chunked=$useChunkedReader") {
      val originalTimeZone = TimeZone.getDefault
      val conf = baseConf("orc")
        .set("spark.rapids.sql.reader.chunked", useChunkedReader.toString)
      val readSchema = StructType(Seq(StructField("ts", TimestampType)))

      try {
        withTempPath { fileRoot =>
          withCpuSparkSession(spark => {
            setSessionTimeZone(spark, "UTC")
            spark.range(1)
              .selectExpr(s"CAST($value AS $sourceType) AS ts")
              .write.orc(fileRoot.getCanonicalPath)
          }, conf = conf)

          val (fromCpu, fromGpu) = runOnCpuAndGpu(
            spark => {
              setSessionTimeZone(spark, "America/Los_Angeles")
              spark.read.schema(readSchema).orc(fileRoot.getCanonicalPath)
            },
            identity,
            conf = conf,
            repart = 0,
            skipCanonicalizationCheck = true,
            existClasses = "GpuFileSourceScanExec")
          compareResults(
            sort = false,
            floatEpsilon = 0.0,
            fromCpu = fromCpu,
            fromGpu = fromGpu)
        }
      } finally {
        TimeZone.setDefault(originalTimeZone)
      }
    }
  }

  Seq(false, true).foreach { useChunkedReader =>
    test(s"coalescing files with different writer timezones, chunked=$useChunkedReader") {
      val originalTimeZone = TimeZone.getDefault
      val conf = baseConf("orc")
        .set(RapidsConf.ORC_READER_TYPE.key, RapidsReaderType.COALESCING.toString)
        .set(RapidsConf.CHUNKED_READER.key, useChunkedReader.toString)
        .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, Integer.MAX_VALUE.toString)
        .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, (1L << 30).toString)
        .set("spark.sql.files.maxPartitionBytes", (1L << 30).toString)

      try {
        withTempPath { fileRoot =>
          val utcPath = new File(fileRoot, "utc")
          val losAngelesPath = new File(fileRoot, "los-angeles")
          withCpuSparkSession(spark => {
            setSessionTimeZone(spark, "UTC")
            fileDataFrame(spark, new Random(1L))
              .coalesce(1)
              .write
              .orc(utcPath.getCanonicalPath)

            setSessionTimeZone(spark, "America/Los_Angeles")
            fileDataFrame(spark, new Random(2L), idOffset = RandomRowCount * 2)
              .coalesce(1)
              .write
              .orc(losAngelesPath.getCanonicalPath)
          }, conf = conf)

          val (fromCpu, fromGpu) = runOnCpuAndGpu(
            spark => {
              setSessionTimeZone(spark, "UTC")
              val df = spark.read.orc(
                utcPath.getCanonicalPath,
                losAngelesPath.getCanonicalPath)
              assert(df.queryExecution.executedPlan.execute().getNumPartitions === 1,
                "the two ORC files must be assigned to one coalescing reader")
              df
            },
            _.orderBy("id"),
            conf = conf,
            repart = 0,
            skipCanonicalizationCheck = true,
            existClasses = "GpuFileSourceScanExec")
          compareResults(
            sort = false,
            floatEpsilon = 0.0,
            fromCpu = fromCpu,
            fromGpu = fromGpu)
        }
      } finally {
        TimeZone.setDefault(originalTimeZone)
      }
    }
  }

  for {
    readerType <- Seq(
      RapidsReaderType.PERFILE,
      RapidsReaderType.COALESCING,
      RapidsReaderType.MULTITHREADED)
    useChunkedReader <- Seq(false, true)
  } {
    test(s"cross-timezone $readerType ORC reader, chunked=$useChunkedReader") {
      val originalTimeZone = TimeZone.getDefault
      val conf = baseConf("orc")
        .set(RapidsConf.ORC_READER_TYPE.key, readerType.toString)
        .set(RapidsConf.CHUNKED_READER.key, useChunkedReader.toString)

      try {
        withTempPath { fileRoot =>
          withCpuSparkSession(spark => {
            setSessionTimeZone(spark, "UTC")
            timestampDataFrame(spark, ExplicitTimestampMicros)
              .coalesce(1)
              .write
              .orc(fileRoot.getCanonicalPath)
          }, conf = conf)

          val (fromCpu, fromGpu) = runOnCpuAndGpu(
            spark => {
              setSessionTimeZone(spark, "America/Los_Angeles")
              spark.read.orc(fileRoot.getCanonicalPath)
            },
            _.orderBy("id"),
            conf = conf,
            repart = 0,
            skipCanonicalizationCheck = true,
            existClasses = "GpuFileSourceScanExec")
          compareResults(
            sort = false,
            floatEpsilon = 0.0,
            fromCpu = fromCpu,
            fromGpu = fromGpu)
        }
      } finally {
        TimeZone.setDefault(originalTimeZone)
      }
    }
  }

  for {
    writerTimeZone <- timezones
    v1SourceList <- v1SourceLists
  } {
    val dsLabel = if (v1SourceList == "orc") "v1" else "v2"
    test(s"ORC timezone matrix ($dsLabel) for writer timezone $writerTimeZone") {
      val originalTimeZone = TimeZone.getDefault
      // Use a fixed seed for reproducibility; tests must not be non-deterministic.
      val runSeed = 42L
      val random = new Random(runSeed)
      val conf = baseConf(v1SourceList)
      val existClass = if (v1SourceList == "orc") "GpuFileSourceScanExec" else "GpuBatchScan"

      try {
        withTempPath { fileRoot =>
          withCpuSparkSession(spark => {
            setSessionTimeZone(spark, writerTimeZone)
            writeFile(spark, fileRoot, random)
          }, conf = conf)

          timezones.foreach { readerTimeZone =>
            withClue(s"writerTimezone=$writerTimeZone readerTimezone=$readerTimeZone " +
                s"datasource=$dsLabel") {
              val (fromCpu, fromGpu) = runOnCpuAndGpu(
                spark => {
                  setSessionTimeZone(spark, readerTimeZone)
                  spark.read.orc(fileRoot.getCanonicalPath)
                },
                _.orderBy("id"),
                conf = conf,
                repart = 0,
                skipCanonicalizationCheck = true,
                existClasses = existClass)
              compareResults(
                sort = false,
                floatEpsilon = 0.0,
                fromCpu = fromCpu,
                fromGpu = fromGpu)
            }
          }
        }
      } finally {
        TimeZone.setDefault(originalTimeZone)
      }
    }
  }
}
