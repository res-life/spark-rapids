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

package com.nvidia.spark.rapids.iceberg.parquet

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.iceberg.shaded.org.apache.parquet.schema.{GroupType, PrimitiveType, Type}

/** Visits physical fields, including the children of opaque logical groups such as VARIANT. */
private[parquet] abstract class ParquetSchemaVisitor[T] {
  def primitive(primitive: PrimitiveType): T

  def group(group: GroupType, children: Seq[T]): T

  /** Completes a field after its children, retaining its physical parent for wrapper handling. */
  def field(field: Type, parent: GroupType, result: T): T = result
}

private[parquet] object ParquetSchemaVisitor {
  // Iceberg's ParquetTypeVisitor gained a VARIANT callback after 1.9. Traverse the raw
  // physical groups here so every supported runtime visits the same fields and IDs.
  def visitFields[T](group: GroupType, visitor: ParquetSchemaVisitor[T]): Seq[T] = {
    group.getFields.asScala.map { field =>
      val result = if (field.isPrimitive) {
        visitor.primitive(field.asPrimitiveType())
      } else {
        val childGroup = field.asGroupType()
        visitor.group(childGroup, visitFields(childGroup, visitor))
      }
      visitor.field(field, group, result)
    }.toSeq
  }

  def fieldIds(schema: GroupType): Set[Int] = {
    val ids = mutable.HashSet.empty[Int]
    visitFields(schema, new ParquetSchemaVisitor[Unit] {
      override def primitive(primitive: PrimitiveType): Unit = ()

      override def group(group: GroupType, children: Seq[Unit]): Unit = ()

      override def field(field: Type, parent: GroupType, result: Unit): Unit = {
        Option(field.getId).foreach(id => ids += id.intValue())
      }
    })
    ids.toSet
  }
}
