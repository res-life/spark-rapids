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

/** Common planning gate for Iceberg format v3 scans and writes. */
object IcebergV3Support {
  private val FormatVersion3 = 3

  def tagForGpu(table: Table, meta: RapidsMeta[_, _, _]): Unit = {
    tagForGpu(ShimUtils.formatVersion(table), meta)
  }

  def tagForGpu(properties: Map[String, String], meta: RapidsMeta[_, _, _]): Unit = {
    val formatVersion = properties.get(TableProperties.FORMAT_VERSION).map(_.toInt).getOrElse(2)
    tagForGpu(formatVersion, meta)
  }

  private def tagForGpu(formatVersion: Int, meta: RapidsMeta[_, _, _]): Unit = {
    if (formatVersion == FormatVersion3 && !meta.conf.isIcebergV3Enabled) {
      meta.willNotWorkOnGpu("Iceberg v3 support is disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_V3} to true")
    }
  }
}
