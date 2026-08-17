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

package com.nvidia.spark.rapids.iceberg.data

import java.util.Objects

import com.nvidia.spark.rapids.iceberg.ShimUtils
import org.apache.iceberg.{DataFile, DeleteFile, FileContent, StructLike}

/** Delete files split by the phase that applies them. */
case class GpuDeleteFileInfo(
    deletionVector: Option[DeleteFile],
    postReadDeletes: Seq[DeleteFile])

object GpuDeleteFileInfo {
  /**
   * Validates task delete files and gives a Puffin deletion vector precedence over legacy
   * position-delete files. Equality deletes remain post-read filters.
   */
  def apply(dataFile: DataFile, deletes: Seq[DeleteFile]): GpuDeleteFileInfo = {
    deletes.find(d => d.content() != FileContent.EQUALITY_DELETES &&
        d.content() != FileContent.POSITION_DELETES).foreach { delete =>
      throw new UnsupportedOperationException(s"Unsupported delete content: ${delete.content()}")
    }

    val (equalityDeletes, positionDeletes) =
      deletes.partition(_.content() == FileContent.EQUALITY_DELETES)
    val (deletionVectors, legacyPositionDeletes) =
      positionDeletes.partition(ShimUtils.isDeletionVector)
    require(deletionVectors.size <= 1,
      s"Expected at most one deletion vector per data file, found ${deletionVectors.size}")

    deletionVectors.headOption.foreach(validateScope(dataFile, _))
    val effectivePositionDeletes =
      if (deletionVectors.nonEmpty) Seq.empty else legacyPositionDeletes
    new GpuDeleteFileInfo(deletionVectors.headOption,
      equalityDeletes ++ effectivePositionDeletes)
  }

  private def validateScope(dataFile: DataFile, deletionVector: DeleteFile): Unit = {
    val dataFilePath = ShimUtils.locationOf(dataFile)
    val referencedDataFile = ShimUtils.referencedDataFile(deletionVector)
    require(referencedDataFile != null,
      s"Deletion vector ${ShimUtils.locationOf(deletionVector)} has no referenced data file")
    require(dataFilePath == referencedDataFile,
      s"Deletion vector ${ShimUtils.locationOf(deletionVector)} references " +
        s"$referencedDataFile, not $dataFilePath")

    val deleteSequenceNumber = deletionVector.dataSequenceNumber()
    val dataSequenceNumber = dataFile.dataSequenceNumber()
    require(deleteSequenceNumber != null && dataSequenceNumber != null &&
        deleteSequenceNumber >= dataSequenceNumber,
      s"Deletion vector sequence number $deleteSequenceNumber must be greater than or equal " +
        s"to data file sequence number $dataSequenceNumber")
    require(deletionVector.specId() == dataFile.specId(),
      s"Deletion vector spec ${deletionVector.specId()} does not match data file spec " +
        s"${dataFile.specId()}")
    require(samePartition(deletionVector.partition(), dataFile.partition()),
      s"Deletion vector partition ${deletionVector.partition()} does not match data file " +
        s"partition ${dataFile.partition()}")
  }

  private def samePartition(left: StructLike, right: StructLike): Boolean = {
    if (left == null || right == null) {
      return left == right
    }

    left.size() == right.size() && (0 until left.size()).forall { index =>
      Objects.equals(left.get(index, classOf[Object]), right.get(index, classOf[Object]))
    }
  }
}
