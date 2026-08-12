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
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.{GpuColumnVector, GpuWrite}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.GpuProjectingColumnarBatch
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{WRITE_OPERATION, WRITE_WITH_METADATA_OPERATION}
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.GpuDelteWritingSparkTask.filterByOperation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuReplaceDataExec(
    inner: SparkPlan,
    refreshCache: () => Unit,
    projections: ReplaceDataProjections,
    write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override lazy val writingTask: GpuWritingSparkTask[_] =
    GpuReplaceDataWritingSparkTask(projections)

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuReplaceDataExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GpuReplaceDataExec = {
    copy(inner = newChild)
  }
}

case class GpuReplaceDataWritingSparkTask(
    projs: ReplaceDataProjections)
  extends GpuWritingSparkTask[DataWriter[ColumnarBatch]] {

  private lazy val rowProjection = GpuProjectingColumnarBatch(projs.rowProjection)
  private lazy val rowDataTypes = rowProjection.schema.fields.map(_.dataType)
  private lazy val metadataProjection = projs.metadataProjection
    .map(GpuProjectingColumnarBatch(_))
    .orNull
  private lazy val metadataDataTypes = projs.metadataProjection
    .map(_.schema.fields.map(_.dataType))
    .orNull

  override protected def write(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    val writeFilter = filterByOperation(batch, WRITE_OPERATION)
    withResource(writeFilter) { _ =>
      withResource(rowProjection.project(batch)) { rows =>
        val filteredRows = GpuColumnVector.filter(rows, rowDataTypes, writeFilter)
        if (filteredRows.numRows() > 0) {
          writer.write(filteredRows)
        } else {
          filteredRows.close()
        }
      }
    }

    if (metadataProjection != null) {
      val writeWithMetadataFilter = filterByOperation(batch, WRITE_WITH_METADATA_OPERATION)
      withResource(writeWithMetadataFilter) { _ =>
        val rows = withResource(rowProjection.project(batch)) { rows =>
          GpuColumnVector.filter(rows, rowDataTypes, writeWithMetadataFilter)
        }

        closeOnExcept(rows) { _ =>
          if (rows.numRows() > 0) {
            val metadata = withResource(metadataProjection.project(batch)) { metadata =>
              GpuColumnVector.filter(metadata, metadataDataTypes, writeWithMetadataFilter)
            }
            writer.write(metadata, rows)
          } else {
            rows.close()
          }
        }
      }
    }
  }
}
