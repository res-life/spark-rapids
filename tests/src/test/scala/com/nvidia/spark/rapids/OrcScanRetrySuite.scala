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

package com.nvidia.spark.rapids

import java.time.ZoneId

import ai.rapids.cudf.{ColumnVector, DType, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}

class OrcScanRetrySuite extends RmmSparkRetrySuiteBase {

  private val timestampSchema = StructType(Seq(StructField("a", TimestampType)))
  private val longSchema = StructType(Seq(StructField("a", LongType)))

  private def injectGpuRetryOom(): Unit = {
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
  }

  private def assertRetrySucceeds(table: Table, tableSchema: StructType): Unit = {
    injectGpuRetryOom()
    withResource(GpuOrcScan.rebaseAndEvolveSchemaWithRetryAndClose(
        table, tableSchema, timestampSchema, isSchemaCaseSensitive = true,
        writerTimezone = ZoneId.of("UTC"), writerUsedProlepticGregorian = true)) { result =>
      assertResult(1)(result.getRowCount)
      assertResult(DType.TIMESTAMP_MICROSECONDS)(result.getColumn(0).getType)
    }
  }

  test("ORC timestamp rebase is retried on OOM") {
    val table = withResource(ColumnVector.fromLongs(0L)) { longs =>
      withResource(longs.castTo(DType.TIMESTAMP_MICROSECONDS)) { timestamps =>
        new Table(timestamps)
      }
    }
    assertRetrySucceeds(table, timestampSchema)
  }

  test("ORC integer-to-timestamp schema evolution is retried on OOM") {
    val table = withResource(ColumnVector.fromLongs(0L)) { longs =>
      new Table(longs)
    }
    assertRetrySucceeds(table, longSchema)
  }
}
