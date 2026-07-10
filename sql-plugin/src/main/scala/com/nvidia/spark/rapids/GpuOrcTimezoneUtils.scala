/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnView, DType, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB
import java.time.{DateTimeException, ZoneId}
import java.util.Optional
import scala.collection.mutable.ArrayBuffer

object GpuOrcTimezoneUtils {

  /**
   * Rebase ORC timestamps considering writer and reader timezones.
   *
   * Uses the JNI kernel `GpuTimeZoneDB.convertOrcTimezones` for both same- and cross-timezone
   * reads. Even when the timezone rules match, the kernel must reconstruct the writer-specific
   * ORC 2015 base before deciding whether to apply the negative nanos borrow.
   *
   * @param input the input table (timestamps read as UTC via ignoreTimezoneInStripeFooter)
   * @param writerTimezone the writer timezone from the ORC stripe footer
   * @return table with rebased timestamp columns; input is closed
   */
  def rebaseOrcTimestamps(input: Table, writerTimezone: String): Table = {
    val readerTz = ZoneId.systemDefault().getId
    // ORC footers can carry legacy/short IDs (e.g. "PST", "CST", "ACT") that
    // ZoneId.of() rejects on its own, so resolve via the SHORT_IDS alias map.
    // We deliberately avoid TimeZone.getTimeZone here because it silently
    // returns "GMT" for any unrecognized id, which would silently corrupt
    // cross-TZ reads. ZoneId.of throws DateTimeException instead.
    val writerTz = if (writerTimezone.isEmpty) {
      readerTz
    } else {
      try {
        ZoneId.of(writerTimezone, ZoneId.SHORT_IDS).getId
      } catch {
        case e: DateTimeException =>
          throw new IllegalArgumentException(
            s"Unrecognized writer timezone in ORC stripe footer: '$writerTimezone'", e)
      }
    }

    rebaseWithWriterTimezone(input, writerTz, readerTz)
  }

  /**
   * Rebase timestamps using the writer and reader timezones.
   *
   * cuDF reads ORC timestamps with `ignoreTimezoneInStripeFooter`, so the base_timestamp
   * is computed in UTC. ORC Java computes base_timestamp in the *writer* timezone, so the
   * millis passed to `convertBetweenTimezones` already encode the writer TZ base offset.
   *
   * To match ORC Java, the JNI `convertOrcTimezones` kernel first applies the writer TZ base
   * offset and recomputes the negative nanos borrow, then applies any writer-to-reader TZ delta.
   */
  private def rebaseWithWriterTimezone(
      input: Table, writerTz: String, readerTz: String): Table = {
    withResource(input) { _ =>
      withResource(GpuTimeZoneDB.buildOrcTimezoneContext(writerTz, readerTz)) { tzCtx =>
        val newColumns = (0 until input.getNumberOfColumns).safeMap { colIdx =>
          val col = input.getColumn(colIdx)
          val dType = col.getType
          if (dType.hasTimeResolution) {
            GpuTimeZoneDB.convertOrcTimezones(col, tzCtx)
          } else if (dType == DType.LIST || dType == DType.STRUCT) {
            withResource(new ArrayBuffer[ColumnView]) { toClose =>
              val rebased = rebaseNestedWithWriterTimezone(col, tzCtx, toClose)
              if (rebased eq col) {
                col.incRefCount()
              } else {
                toClose += rebased
                rebased.copyToColumnVector()
              }
            }
          } else {
            col.incRefCount()
          }
        }
        withResource(newColumns) { _ =>
          new Table(newColumns: _*)
        }
      }
    }
  }

  private def rebaseNestedWithWriterTimezone(
      col: ColumnView,
      tzCtx: GpuTimeZoneDB.OrcTimezoneContext,
      toClose: ArrayBuffer[ColumnView]): ColumnView = {
    val addToClose = (v: ColumnView) => { toClose += v; v }
    val dType = col.getType

    if (dType.hasTimeResolution) {
      GpuTimeZoneDB.convertOrcTimezones(col, tzCtx)
    } else if (dType == DType.LIST) {
      val child = addToClose(col.getChildColumnView(0))
      val newChild = rebaseNestedWithWriterTimezone(child, tzCtx, toClose)
      if (newChild ne child) {
        col.replaceListChild(addToClose(newChild))
      } else {
        col
      }
    } else if (dType == DType.STRUCT) {
      val newViews = (0 until col.getNumChildren).safeMap { i =>
        val child = addToClose(col.getChildColumnView(i))
        val newChild = rebaseNestedWithWriterTimezone(child, tzCtx, toClose)
        if (newChild ne child) addToClose(newChild)
        newChild
      }
      val opNullCount = Optional.of(col.getNullCount.asInstanceOf[java.lang.Long])
      new ColumnView(col.getType, col.getRowCount, opNullCount, col.getValid,
        col.getOffsets, newViews.toArray)
    } else {
      col
    }
  }
}
