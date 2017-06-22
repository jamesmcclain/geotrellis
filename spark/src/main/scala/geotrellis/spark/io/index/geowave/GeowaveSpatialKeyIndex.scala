/*
 * Copyright 2017 Azavea
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

package geotrellis.spark.io.index.geowave

import geotrellis.spark._
import geotrellis.spark.io.index.KeyIndex

import mil.nga.giat.geowave.core.index._
import mil.nga.giat.geowave.core.index.dimension._
import mil.nga.giat.geowave.core.index.sfc.data.{ BasicNumericDataset, NumericRange }
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType
import mil.nga.giat.geowave.core.index.sfc.tiered.{ TieredSFCIndexFactory, TieredSFCIndexStrategy }

import scala.collection.JavaConverters._


object GeowaveSpatialKeyIndex {
  def apply(minKey: SpatialKey, maxKey: SpatialKey, spatialResolution: Int): GeowaveSpatialKeyIndex =
    apply(new KeyBounds(minKey, maxKey), spatialResolution)

  def apply(keyBounds: KeyBounds[SpatialKey], spatialResolution: Int): GeowaveSpatialKeyIndex =
    apply(keyBounds, spatialResolution, spatialResolution)

  def apply(keyBounds: KeyBounds[SpatialKey], xResolution: Int, yResolution: Int): GeowaveSpatialKeyIndex =
    new GeowaveSpatialKeyIndex(keyBounds, xResolution, yResolution)
}

/**
  * Class that provides spatial indexing using the GeoWave indexing
  * machinery.
  *
  * @param   keyBounds    The bounds over-which the index is valid
  * @param   xResolution  The number of bits of resolution requested/required by the x-axis
  * @param   yResoltuion  The number of bits of resolution requested/required by the y-axis
  * @author  James McClain
  */
class GeowaveSpatialKeyIndex(val keyBounds: KeyBounds[SpatialKey], val xResolution: Int, val yResolution: Int) extends KeyIndex[SpatialKey] {

  val maxRangeDecomposition = 5000

  val KeyBounds(SpatialKey(minCol, minRow), SpatialKey(maxCol, maxRow)) = keyBounds
  @transient lazy val dim1 = new SFCDimensionDefinition(new BasicDimensionDefinition(minCol, maxCol), xResolution)
  @transient lazy val dim2 = new SFCDimensionDefinition(new BasicDimensionDefinition(minRow, maxRow), yResolution)
  @transient lazy val dimensions: Array[SFCDimensionDefinition] = Array(dim1, dim2)
  @transient lazy val strategy: TieredSFCIndexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(dimensions, SFCType.HILBERT)

  private def idToLong(id: Array[Byte]): Long = {
    val bytes =
      id
        .drop(1) // drop tier byte
        .take(7) // take seven most significant bytes
    BigInt(Array[Byte](1) ++ bytes).toLong // Tack on MSB to make sure number is positive
  }

  def toIndex(key: SpatialKey): Long = {
    val SpatialKey(col, row) = key
    val range1 = new NumericRange(col, col)
    val range2 = new NumericRange(row, row)
    val multiRange = new BasicNumericDataset(Array(range1, range2))
    val insertionIds = strategy.getInsertionIds(multiRange)

    assert(insertionIds.size() == 1)
    idToLong(insertionIds.get(0).getBytes())
  }

  def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(Long, Long)] = {
    val (SpatialKey(col1, row1), SpatialKey(col2, row2)) = keyRange
    val minCol = math.min(col1, col2)
    val maxCol = math.max(col1, col2)
    val minRow = math.min(row1, row2)
    val maxRow = math.max(row1, row2)

    val range1 = new NumericRange(minCol, maxCol)
    val range2 = new NumericRange(minRow, maxRow)
    val multiRange = new BasicNumericDataset(Array(range1, range2))
    val queryRanges = strategy.getQueryRanges(multiRange, maxRangeDecomposition)

    queryRanges
      .asScala
      .map({ range: ByteArrayRange =>
        val start = range.getStart()
        val end = range.getEnd()
        (idToLong(start.getBytes()), idToLong(end.getBytes()))
      })
  }
}
