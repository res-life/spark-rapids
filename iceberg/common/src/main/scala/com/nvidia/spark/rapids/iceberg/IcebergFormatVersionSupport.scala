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

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{RapidsConf, RapidsMeta}
import org.apache.iceberg.{Schema, Table, TableProperties}
import org.apache.iceberg.types.{Types, TypeUtil}

import org.apache.spark.sql.execution.SparkPlan

/** Common planning gate for Iceberg table format versions. */
object IcebergFormatVersionSupport {
  private val MaxSupportedFormatVersion = 2
  private val SupportedDefaultTypeIds = Set(
    "BOOLEAN", "INTEGER", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP",
    "STRING", "BINARY", "DECIMAL")

  def tagForFormatVersion(table: Table, meta: RapidsMeta[_, _, _]): Unit = {
    tagForFormatVersion(table, table.schema(), meta)
  }

  def tagForFormatVersion(
      table: Table,
      schemaToCheck: Schema,
      meta: RapidsMeta[_, _, _]): Unit = {
    val formatVersion = ShimUtils.formatVersion(table)
    tagForFormatVersion(formatVersion, meta)
    if (formatVersion > MaxSupportedFormatVersion && meta.conf.isIcebergV3Enabled) {
      unsupportedDefault(schemaToCheck).foreach { case (path, typeName) =>
        meta.willNotWorkOnGpu(
          s"Iceberg default for field '$path' with type $typeName is not supported on GPU")
      }
    }
  }

  def tagForFormatVersion(
      properties: Map[String, String],
      meta: RapidsMeta[_, _, _]): Unit = {
    val formatVersion = properties.get(TableProperties.FORMAT_VERSION).map(_.toInt).getOrElse(2)
    tagForFormatVersion(formatVersion, meta)
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

  private[iceberg] def unsupportedDefault(schema: Schema): Option[(String, String)] = {
    def find(fields: Seq[Types.NestedField], parent: String): Option[(String, String)] = {
      fields.iterator.map { field =>
        val path = if (parent.isEmpty) field.name() else s"$parent.${field.name()}"
        val hasDefault =
          ShimUtils.hasInitialDefault(field) || ShimUtils.hasWriteDefault(field)
        val fieldType = field.`type`()

        val unsupportedTimestampNtz = fieldType == Types.TimestampType.withoutZone()
        if (hasDefault && fieldType.isPrimitiveType &&
            (!SupportedDefaultTypeIds.contains(fieldType.typeId().name()) ||
              unsupportedTimestampNtz)) {
          Some(path -> fieldType.toString)
        } else if (hasDefault && !fieldType.isPrimitiveType && !fieldType.isStructType) {
          Some(path -> fieldType.toString)
        } else if (fieldType.isNestedType) {
          find(fieldType.asNestedType().fields().asScala.toSeq, path)
        } else {
          None
        }
      }.collectFirst { case Some(value) => value }
    }

    find(schema.columns().asScala.toSeq, "")
  }

  def withRequiredFields(
      tableSchema: Schema,
      expectedSchema: Schema,
      requiredFieldIds: Seq[Int]): Schema = {
    val projectedIds = TypeUtil.getProjectedIds(expectedSchema).asScala.map(_.toInt).toSet
    val missingFields = requiredFieldIds.distinct.filterNot(projectedIds).map { fieldId =>
      Option(tableSchema.findField(fieldId)).getOrElse {
        throw new IllegalArgumentException(s"Cannot find required field for ID $fieldId")
      }
    }

    if (missingFields.isEmpty) {
      expectedSchema
    } else {
      new Schema((expectedSchema.columns().asScala ++ missingFields).asJava)
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
