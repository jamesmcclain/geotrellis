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

import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.{ Unit => BinUnit }
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition
import mil.nga.giat.geowave.core.index._
import mil.nga.giat.geowave.core.index.dimension._
import mil.nga.giat.geowave.core.index.sfc.data.{ BasicNumericDataset, NumericRange }
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType
import mil.nga.giat.geowave.core.index.sfc.tiered.{ TieredSFCIndexFactory, TieredSFCIndexStrategy }

import scala.collection.JavaConverters._


sealed trait GeowaveBinUnit { def unit: BinUnit; def name: String }
case class GeowaveDecade() extends GeowaveBinUnit { val unit = BinUnit.DECADE; val name = "decade" }
case class GeowaveYear() extends GeowaveBinUnit { val unit = BinUnit.YEAR; val name = "year" }
case class GeowaveMonth() extends GeowaveBinUnit { val unit = BinUnit.MONTH; val name = "month" }
case class GeowaveWeek() extends GeowaveBinUnit { val unit = BinUnit.WEEK; val name = "week" }
case class GeowaveDay() extends GeowaveBinUnit { val unit = BinUnit.DAY; val name = "day" }
case class GeowaveHour() extends GeowaveBinUnit { val unit = BinUnit.HOUR; val name = "hour" }
case class GeowaveMinute() extends GeowaveBinUnit { val unit = BinUnit.MINUTE; val name = "minute" }

object GeowaveSpaceTimeKeyIndex {
  def apply(
    minKey: SpaceTimeKey, maxKey: SpaceTimeKey,
    spatialResolution: Int, temporalResolution: Int
  ): GeowaveSpaceTimeKeyIndex =
    apply(KeyBounds(minKey, maxKey), spatialResolution, temporalResolution, GeowaveYear())

  def apply(
    minKey: SpaceTimeKey, maxKey: SpaceTimeKey,
    spatialResolution: Int, temporalResolution: Int,
    unit: GeowaveBinUnit
  ): GeowaveSpaceTimeKeyIndex =
    apply(KeyBounds(minKey, maxKey), spatialResolution, temporalResolution, unit)

  def apply(
    keyBounds: KeyBounds[SpaceTimeKey],
    spatialResolution: Int, temporalResolution: Int,
    unit: GeowaveBinUnit
  ): GeowaveSpaceTimeKeyIndex =
    apply(keyBounds, spatialResolution, spatialResolution, temporalResolution, 1, unit)

  def apply(
    keyBounds: KeyBounds[SpaceTimeKey],
    xResolution: Int,
    yResolution: Int,
    temporalResolution: Int,
    epochBytes: Int,
    unit: String
  ): GeowaveSpaceTimeKeyIndex = {
    val binUnit = unit match {
      case "decade" => GeowaveDecade()
      case "year" => GeowaveYear()
      case "month" => GeowaveMonth()
      case "week" => GeowaveWeek()
      case "day" => GeowaveDay()
      case "hour" => GeowaveHour()
      case "minute" => GeowaveMinute()
      case _ => throw new Exception
    }

    new GeowaveSpaceTimeKeyIndex(keyBounds, xResolution, yResolution, temporalResolution, epochBytes, binUnit)
  }

  def apply(
    keyBounds: KeyBounds[SpaceTimeKey],
    xResolution: Int,
    yResolution: Int,
    temporalResolution: Int,
    epochBytes: Int,
    unit: GeowaveBinUnit
  ): GeowaveSpaceTimeKeyIndex =
    new GeowaveSpaceTimeKeyIndex(keyBounds, xResolution, yResolution, temporalResolution, epochBytes, unit)
}

/**
  * Class that provides spatio-temporal indexing using the GeoWave
  * indexing machinery.
  *
  * @param   keyBounds           The bounds over-which the index is valid
  * @param   xResolution         The number of bits of resolution requested/required by the x-axis
  * @param   yResoltuion         The number of bits of resolution requested/required by the y-axis
  * @param   temporalResolution  The number of bits of resolution requested/required for temporal bins
  * @param   epochBytes          The number of epoch bytes to keep
  * @param   unit                The length (in time) of temporal bins
  * @author  James McClain
  */
class GeowaveSpaceTimeKeyIndex(
  val keyBounds: KeyBounds[SpaceTimeKey],
  val xResolution: Int,
  val yResolution: Int,
  val temporalResolution: Int,
  val epochBytes: Int,
  val unit: GeowaveBinUnit
) extends KeyIndex[SpaceTimeKey] {

  val maxRangeDecomposition = 5000

  require((0 <= epochBytes) && (epochBytes <= 4))

  val KeyBounds(SpaceTimeKey(minCol, minRow, minTime), SpaceTimeKey(maxCol, maxRow, maxTime)) = keyBounds
  @transient lazy val dim1 = new SFCDimensionDefinition(new BasicDimensionDefinition(minCol, maxCol), xResolution)
  @transient lazy val dim2 = new SFCDimensionDefinition(new BasicDimensionDefinition(minRow, maxRow), yResolution)
  @transient lazy val dim3 = new SFCDimensionDefinition(new TimeDefinition(unit.unit), temporalResolution)
  @transient lazy val dimensions: Array[SFCDimensionDefinition] = Array(dim1, dim2, dim3)
  @transient lazy val strategy: TieredSFCIndexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(dimensions, SFCType.HILBERT)

  // Arrays SEEM TO BE big endian
  private def idToLong(id: Array[Byte]): Long = {
    id
      .drop(1)              // drop tier byte
      .drop(4 - epochBytes) // only keep the given number of epoch bytes
      .take(8)              // take eight most significant bytes
      .foldLeft(0L)({ (accumulator, value) => (accumulator << 8) + value.toLong })
  }

  // ASSUMED to be used for insertion
  def toIndex(key: SpaceTimeKey): Long = {
    val SpaceTimeKey(col, row, millis) = key
    val range1 = new NumericRange(col, col)
    val range2 = new NumericRange(row, row)
    val range3 = new NumericRange(millis, millis)
    val multiRange = new BasicNumericDataset(Array(range1, range2, range3))
    val insertionIds = strategy.getInsertionIds(multiRange)

    assert(insertionIds.size() == 1)
    idToLong(insertionIds.get(0).getBytes())
  }

  // ASSUMED to be used for queries
  def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(Long, Long)] = {
    val (SpaceTimeKey(col1, row1, millis1), SpaceTimeKey(col2, row2, millis2)) = keyRange
    val minCol = math.min(col1, col2)
    val maxCol = math.max(col1, col2)
    val minRow = math.min(row1, row2)
    val maxRow = math.max(row1, row2)
    val minMillis = math.min(millis1, millis2)
    val maxMillis = math.max(millis1, millis2)

    val range1 = new NumericRange(minCol, maxCol)
    val range2 = new NumericRange(minRow, maxRow)
    val range3 = new NumericRange(minMillis, maxMillis)
    val multiRange = new BasicNumericDataset(Array(range1, range2, range3))
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
