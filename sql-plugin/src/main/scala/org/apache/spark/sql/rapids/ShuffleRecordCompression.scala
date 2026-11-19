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
package org.apache.spark.sql.rapids

import java.io.OutputStream

import com.nvidia.spark.rapids.{Lz4SegmentedOutputStream, SegmentedByteArrayOutputStream}
import net.jpountz.lz4.LZ4Factory
import net.jpountz.xxhash.XXHashFactory

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.IO_COMPRESSION_LZ4_BLOCKSIZE
import org.apache.spark.io.{CompressionCodec, LZ4CompressionCodec}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.ShuffleBlockId

private object ShuffleRecordCompression {
  // Cache factory discovery, including the native-library fallback, as Spark's codec does.
  lazy val compressor = LZ4Factory.fastestInstance().fastCompressor()
  lazy val hashes = XXHashFactory.fastestInstance()
  val checksumSeed: Int = 0x9747b28c
}

/** Narrow POC: preserve SerializerManager for encryption, other codecs and small segments. */
private[rapids] class ShuffleRecordCompression(conf: SparkConf, manager: SerializerManager) {
  // Spark Core compression settings are fixed for the running application. Like Spark's
  // LZ4CompressionCodec, capture them once, then share the immutable parameters across records.
  private lazy val useLz4 = {
    if (manager.encryptionEnabled || !conf.getBoolean("spark.shuffle.compress", true)) {
      false
    } else {
      val codecName = CompressionCodec.getCodecName(conf)
      codecName.equalsIgnoreCase("lz4") || codecName == classOf[LZ4CompressionCodec].getName
    }
  }
  private lazy val lz4 = new Lz4SegmentedOutputStream.Factory(
    conf.get(IO_COMPRESSION_LZ4_BLOCKSIZE).toInt, ShuffleRecordCompression.compressor)

  def wrapStream(block: ShuffleBlockId, out: OutputStream): OutputStream = out match {
    case segmented: SegmentedByteArrayOutputStream
        if useLz4 && lz4.supports(segmented.getSegmentSize) =>
      lz4.create(segmented,
        ShuffleRecordCompression.hashes.newStreamingHash32(
          ShuffleRecordCompression.checksumSeed).asChecksum())
    case _ => manager.wrapStream(block, out)
  }
}
