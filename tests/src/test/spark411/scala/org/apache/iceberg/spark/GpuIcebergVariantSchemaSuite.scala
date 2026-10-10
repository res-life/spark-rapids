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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark

import com.nvidia.spark.rapids.SchemaUtils._
import com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded
import org.apache.iceberg.Schema
import org.apache.iceberg.shaded.org.apache.parquet.schema.{LogicalTypeAnnotation =>
  ShadedLogicalTypeAnnotation}
import org.apache.iceberg.types.Types
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType, VariantType}

class GpuIcebergVariantSchemaSuite extends AnyFunSuite {

  test("toSparkType preserves field IDs around top-level and nested Variant fields") {
    val payload = Types.StructType.of(
      Types.NestedField.optional(3, "items",
        Types.ListType.ofOptional(4, Types.VariantType.get())),
      Types.NestedField.optional(5, "attributes",
        Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.VariantType.get())))
    val schema = new Schema(
      Types.NestedField.optional(1, "variant_value", Types.VariantType.get()),
      Types.NestedField.optional(2, "payload", payload))

    val sparkSchema = GpuTypeToSparkType.toSparkType(schema)
    val variantField = sparkSchema("variant_value")
    assert(variantField.dataType == VariantType)
    assert(variantField.metadata.getLong(FIELD_ID_METADATA_KEY) == 1L)

    val payloadField = sparkSchema("payload")
    assert(payloadField.metadata.getLong(FIELD_ID_METADATA_KEY) == 2L)
    val payloadType = payloadField.dataType.asInstanceOf[StructType]

    val items = payloadType("items")
    assert(items.metadata.getLong(FIELD_ID_METADATA_KEY) == 3L)
    assert(items.metadata.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 4L)
    assert(items.dataType == ArrayType(VariantType, containsNull = true))

    val attributes = payloadType("attributes")
    assert(attributes.metadata.getLong(FIELD_ID_METADATA_KEY) == 5L)
    assert(attributes.metadata.getLong(MAP_KEY_FIELD_ID_METADATA_KEY) == 6L)
    assert(attributes.metadata.getLong(MAP_VALUE_FIELD_ID_METADATA_KEY) == 7L)
    assert(attributes.dataType ==
      MapType(StringType, VariantType, valueContainsNull = true))
  }

  test("unshade preserves the Variant logical type spec version") {
    val shaded = ShadedLogicalTypeAnnotation.variantType(1.toByte)
    val unshaded = FromIcebergShaded.unshade(shaded)
      .asInstanceOf[LogicalTypeAnnotation.VariantLogicalTypeAnnotation]

    assert(unshaded.getSpecVersion == 1.toByte)
  }
}
