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

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.{GpuColumnVector, GpuMetric, GpuSemaphore, NvtxWithMetrics}
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.hybrid.{CoalesceBatchConverter => NativeConverter}
import com.nvidia.spark.rapids.hybrid.RapidsHostColumn

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * The Iterator wrapper of the underlying NativeConverter which produces the coalesced Batch of
 * HostColumnVectors. The iterator produces RapidsHostColumn instead of HostColumnVector for
 * carrying metadata about [Pinned|Pageable]MemCpy which are displayed as Spark SQL Metrics.
 */
class CoalesceConvertIterator(cpuScanIter: Iterator[ColumnarBatch],
                              targetBatchSizeInBytes: Long,
                              schema: StructType,
                              metrics: Map[String, GpuMetric])
  extends Iterator[Array[RapidsHostColumn]] with Logging {

  private var converterImpl: NativeConverter = _

  private var srcExhausted = false

  private val converterMetrics = Map(
    "C2COutputSize" -> GpuMetric.unwrap(metrics("C2COutputSize")))

  override def hasNext(): Boolean = {
    // isDeckFilled means if there is unconverted source data remained on the deck.
    // hasProceedingBuilders means if there exists working target vectors not being flushed yet.
    val selfHoldData = Option(converterImpl).exists { c =>
      c.isDeckFilled || c.hasProceedingBuilders
    }
    // Check the srcExhausted at first, so as to minimize the potential cost of unnecessary call of
    // prev.hasNext
    lazy val upstreamHoldData = !srcExhausted && cpuScanIter.hasNext
    // Either converter holds data or upstreaming iterator holds data.
    if (selfHoldData || upstreamHoldData) {
      return true
    }

    if (!srcExhausted) {
      srcExhausted = true
    }
    // Close the native Converter and dump column-level metrics for performance inspection.
    Option(converterImpl).foreach { c =>
      // VeloxBatchConverter collects the eclipsedTime of C2C_Conversion by itself.
      // Here we fetch the final value before closing it.
      metrics("C2CTime") += c.eclipsedNanoSecond
      // release the native instance when upstreaming iterator has been exhausted
      val detailedMetrics = c.close()
      val tID = TaskContext.get().taskAttemptId()
      logInfo(s"task[$tID] CoalesceNativeConverter finished:\n$detailedMetrics")
      converterImpl = null
    }
    false
  }

  override def next(): Array[RapidsHostColumn] = {
    require(!srcExhausted, "Please call hasNext in previous to ensure there are remaining data")

    // Initialize the nativeConverter with the first input batch
    if (converterImpl == null) {
      converterImpl = NativeConverter(
        cpuScanIter.next(),
        targetBatchSizeInBytes,
        schema,
        converterMetrics
      )
    }

    // Keeps consuming input batches of cpuScanIter until targetVectors reaches `targetBatchSize`
    // or cpuScanIter being exhausted.
    while (true) {
      val needFlush = if (cpuScanIter.hasNext) {
        metrics("CpuReaderBatches") += 1
        // The only condition leading to a nonEmpty deck is targetVectors are unset after
        // the previous flushing
        if (converterImpl.isDeckFilled) {
          converterImpl.setupTargetVectors()
        }
        // tryAppendBatch, if failed which indicates the remaining space of targetVectors is NOT
        // enough the current input batch, then the batch will be placed on the deck and trigger
        // the flush of working targetVectors
        !converterImpl.tryAppendBatch(cpuScanIter.next())
      } else {
        // If cpuScanIter is exhausted, then flushes targetVectors as the last output item.
        srcExhausted = true
        true
      }
      if (needFlush) {
        metrics("CoalescedBatches") += 1
        val rapidsHostBatch = converterImpl.flush()
        // It is essential to check and tidy up the deck right after flushing. Because if
        // the next call of cpuScanIter.hasNext will release the batch which the deck holds
        // its reference.
        if (converterImpl.isDeckFilled) {
          converterImpl.setupTargetVectors()
        }
        return rapidsHostBatch
      }
    }

    throw new RuntimeException("should NOT reach this line")
  }
}

object CoalesceConvertIterator extends Logging {

  def hostToDevice(hostIter: Iterator[Array[RapidsHostColumn]],
                   outputAttr: Seq[Attribute],
                   metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    val dataTypes = outputAttr.map(_.dataType).toArray

    hostIter.map { hostVectors =>
      Option(TaskContext.get()).foreach { ctx =>
        withResource(new NvtxWithMetrics("gpuAcquireC2C", NvtxColor.GREEN,
          metrics("GpuAcquireTime"))) { _ =>
          GpuSemaphore.acquireIfNecessary(ctx)
        }
      }

      val deviceVectors: Array[ColumnVector] = hostVectors.zip(dataTypes).safeMap {
        case (RapidsHostColumn(hcv, isPinned, totalBytes), dt) =>
          val nvtxMetric = if (isPinned) {
            metrics("PinnedH2DSize") += totalBytes
            new NvtxWithMetrics("pinnedH2D", NvtxColor.DARK_GREEN, metrics("PinnedH2DTime"))
          } else {
            metrics("PageableH2DSize") += totalBytes
            new NvtxWithMetrics("PageableH2D", NvtxColor.GREEN, metrics("PageableH2DTime"))
          }
          withResource(hcv) { _ =>
            withResource(nvtxMetric) { _ =>
              GpuColumnVector.from(hcv.copyToDevice(), dt)
            }
          }
      }

      new ColumnarBatch(deviceVectors, hostVectors.head.vector.getRowCount.toInt)
    }
  }

}