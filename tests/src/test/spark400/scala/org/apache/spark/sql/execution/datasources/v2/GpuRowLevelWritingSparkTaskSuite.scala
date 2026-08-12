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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector => CudfColumnVector}
import com.nvidia.spark.rapids.{GpuColumnVector, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.catalyst.util.{ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{INSERT_OPERATION, REINSERT_OPERATION,
  WRITE_OPERATION, WRITE_WITH_METADATA_OPERATION}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, DeltaWriter,
  WriterCommitMessage}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuRowLevelWritingSparkTaskSuite
    extends RmmSparkRetrySuiteBase with MockitoSugar {

  private val intSchema = StructType(Seq(StructField("value", IntegerType)))

  private def projection(ordinal: Int): ProjectingInternalRow = {
    ProjectingInternalRow(intSchema, Seq(ordinal))
  }

  private def buildBatch(columns: Seq[Seq[Int]]): ColumnarBatch = {
    val gpuColumns = columns.map { values =>
      GpuColumnVector.from(CudfColumnVector.fromInts(values: _*), IntegerType)
    }
    new ColumnarBatch(gpuColumns.toArray, columns.head.length)
  }

  private def runTask(
      task: GpuWritingSparkTask[_],
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    val context = mock[TaskContext]
    when(context.stageId()).thenReturn(0)
    when(context.stageAttemptNumber()).thenReturn(0)
    when(context.partitionId()).thenReturn(0)
    when(context.taskAttemptId()).thenReturn(0L)
    when(context.attemptNumber()).thenReturn(0)

    task.run(
      new TestDataWriterFactory(writer),
      context,
      Iterator.single(batch),
      useCommitCoordinator = false,
      Map.empty[String, SQLMetric])
  }

  test("replace data preserves metadata writes") {
    val writer = new RecordingDataWriter
    val projections = ReplaceDataProjections(
      rowProjection = projection(1),
      metadataProjection = Some(projection(2)))
    val task = GpuReplaceDataWritingSparkTask(projections)

    withResource(buildBatch(Seq(
      Seq(WRITE_OPERATION, WRITE_WITH_METADATA_OPERATION),
      Seq(10, 20),
      Seq(100, 200)))) { batch =>
      runTask(task, writer, batch)
    }

    assert(writer.calls === Seq("write:1", "writeWithMetadata:1"))
  }

  test("delta writes distinguish inserts from reinserts") {
    val projections = WriteDeltaProjections(
      rowProjection = Some(projection(1)),
      rowIdProjection = projection(2),
      metadataProjection = Some(projection(3)))

    Seq(
      GpuDeltaWritingSparkTask(projections) -> Seq("insert:1", "reinsertWithoutMetadata:1"),
      GpuDeltaWithMetadataWritingSparkTask(projections) ->
        Seq("insert:1", "reinsertWithMetadata:1")).foreach { case (task, expectedCalls) =>
      val writer = new RecordingDeltaWriter
      runTask(task, writer, buildBatch(Seq(
        Seq(INSERT_OPERATION, REINSERT_OPERATION),
        Seq(10, 20),
        Seq(100, 200),
        Seq(1000, 2000))))
      assert(writer.calls === expectedCalls)
    }
  }

  private class TestDataWriterFactory(writer: DataWriter[ColumnarBatch])
      extends DataWriterFactory {
    override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
      writer.asInstanceOf[DataWriter[InternalRow]]
    }
  }

  private class RecordingDataWriter extends DataWriter[ColumnarBatch] {
    val calls: ArrayBuffer[String] = ArrayBuffer.empty

    override def write(record: ColumnarBatch): Unit = {
      calls += s"write:${record.numRows()}"
      record.close()
    }

    override def write(metadata: ColumnarBatch, record: ColumnarBatch): Unit = {
      calls += s"writeWithMetadata:${record.numRows()}"
      metadata.close()
      record.close()
    }

    override def commit(): WriterCommitMessage = null
    override def abort(): Unit = {}
    override def close(): Unit = {}
  }

  private class RecordingDeltaWriter extends DeltaWriter[ColumnarBatch] {
    val calls: ArrayBuffer[String] = ArrayBuffer.empty

    override def delete(metadata: ColumnarBatch, id: ColumnarBatch): Unit = {
      if (metadata != null) {
        metadata.close()
      }
      id.close()
    }

    override def update(
        metadata: ColumnarBatch,
        id: ColumnarBatch,
        row: ColumnarBatch): Unit = {
      if (metadata != null) {
        metadata.close()
      }
      id.close()
      row.close()
    }

    override def insert(row: ColumnarBatch): Unit = {
      calls += s"insert:${row.numRows()}"
      row.close()
    }

    override def reinsert(metadata: ColumnarBatch, row: ColumnarBatch): Unit = {
      calls += (if (metadata == null) {
        s"reinsertWithoutMetadata:${row.numRows()}"
      } else {
        s"reinsertWithMetadata:${row.numRows()}"
      })
      if (metadata != null) {
        metadata.close()
      }
      row.close()
    }

    override def commit(): WriterCommitMessage = null
    override def abort(): Unit = {}
    override def close(): Unit = {}
  }
}
