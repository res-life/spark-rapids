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

  private def tagForFormatVersion(formatVersion: Int, meta: RapidsMeta[_, _, _]): Unit = {
    if (formatVersion > MaxSupportedFormatVersion && !meta.conf.isIcebergV3Enabled) {
      meta.willNotWorkOnGpu(s"Iceberg table format version $formatVersion is not supported. " +
        s"To enable set ${RapidsConf.ENABLE_ICEBERG_V3} to true")
    }
  }
}
