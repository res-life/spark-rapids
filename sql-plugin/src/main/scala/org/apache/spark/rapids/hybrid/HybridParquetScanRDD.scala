/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.hybrid

import com.nvidia.spark.rapids.{CoalesceSizeGoal, GpuMetric}

import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class HybridParquetScanRDD(scanRDD: RDD[ColumnarBatch],
                           outputAttr: Seq[Attribute],
                           outputSchema: StructType,
                           coalesceGoal: CoalesceSizeGoal,
                           metrics: Map[String, GpuMetric],
                         ) extends RDD[InternalRow](scanRDD.sparkContext, Nil) {

  override protected def getPartitions: Array[Partition] = scanRDD.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    // the wrapping Iterator for the underlying HybridNativeScan task
    val hybridScanIter = scanRDD.compute(split, context)

    val schema = StructType(outputAttr.map { ar =>
      StructField(ar.name, ar.dataType, ar.nullable)
    })
    val hostResultIter = new CoalesceConvertIterator(
      hybridScanIter,
      coalesceGoal.targetSizeBytes,
      schema,
      metrics
    )
    val deviceIter = CoalesceConvertIterator.hostToDevice(hostResultIter, outputAttr, metrics)

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context, deviceIter.asInstanceOf[Iterator[InternalRow]])
  }
}