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

package com.nvidia.spark.rapids.iceberg

import com.nvidia.spark.rapids.{RapidsConf, RapidsMeta}
import org.apache.iceberg.{Table, TableProperties}

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, NullType, StructType}

/** Common planning gate for Iceberg table format versions. */
object IcebergFormatVersionSupport {
  private val MaxSupportedFormatVersion = 2

  def tagForFormatVersion(table: Table, meta: RapidsMeta[_, _, _]): Unit = {
    tagForFormatVersion(ShimUtils.formatVersion(table), meta)
  }

  def tagForFormatVersion(
      properties: Map[String, String],
      meta: RapidsMeta[_, _, _]): Unit = {
    val formatVersion = properties.get(TableProperties.FORMAT_VERSION).map(_.toInt).getOrElse(2)
    tagForFormatVersion(formatVersion, meta)
  }

  /**
   * Tags operations containing Spark NullType when the active Iceberg runtime cannot expose
   * Iceberg's v3 unknown type. Returns true when planning may continue through v3 type handling.
   */
  def tagForUnknownType(
      schema: StructType,
      meta: RapidsMeta[_, _, _]): Boolean = {
    val supported = !containsUnknownType(schema) || ShimUtils.supportsUnknownType()
    if (!supported) {
      meta.willNotWorkOnGpu(
        "Iceberg unknown type is not supported by the active Iceberg runtime")
    }
    supported
  }

  private def containsUnknownType(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case StructType(fields) => fields.exists(field => containsUnknownType(field.dataType))
    case ArrayType(elementType, _) => containsUnknownType(elementType)
    case MapType(keyType, valueType, _) =>
      containsUnknownType(keyType) || containsUnknownType(valueType)
    case _ => false
  }

  private def tagForFormatVersion(formatVersion: Int, meta: RapidsMeta[_, _, _]): Unit = {
    if (formatVersion > MaxSupportedFormatVersion && !meta.conf.isIcebergV3Enabled) {
      val reason = s"Iceberg table format version $formatVersion is not supported. " +
        s"To enable set ${RapidsConf.ENABLE_ICEBERG_V3} to true"
      meta.willNotWorkOnGpu(reason)

      // A scan is tagged before its enclosing SparkPlan nodes. Keep MergeRowsExec on CPU with a
      // v3 target because v3 row-lineage metadata changes the row schemas it processes. This is
      // especially important with AQE, where the CPU V2 write and its query can be planned in
      // separate passes.
      tagMergeRowsAncestor(meta.parent, reason)
    }
  }

  private def tagMergeRowsAncestor(
      meta: Option[RapidsMeta[_, _, _]],
      reason: String): Unit = meta match {
    case Some(current) =>
      current.wrapped match {
        case plan: SparkPlan if plan.getClass.getSimpleName == "MergeRowsExec" =>
          plan.setTagValue(RapidsMeta.gpuSupportedTag,
            plan.getTagValue(RapidsMeta.gpuSupportedTag).getOrElse(Set.empty) + reason)
        case _ => tagMergeRowsAncestor(current.parent, reason)
      }
    case None =>
  }
}
