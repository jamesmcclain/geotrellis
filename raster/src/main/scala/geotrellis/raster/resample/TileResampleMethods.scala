/*
 * Copyright 2016 Azavea
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

package geotrellis.raster.resample

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.util.MethodExtensions


trait TileResampleMethods[T <: CellGrid[Int]] extends MethodExtensions[T] {
  def resample(extent: Extent, target: RasterExtent, method: ResampleMethod): T

  def resample(extent: Extent, target: RasterExtent): T =
    resample(extent, target, ResampleMethod.DEFAULT)


  def resample(extent: Extent, targetCols: Int, targetRows: Int, method: ResampleMethod): T

  def resample(extent: Extent, targetCols: Int, targetRows: Int): T =
    resample(extent, targetCols, targetRows, ResampleMethod.DEFAULT)


  def resample(targetCols: Int, targetRows: Int, method: ResampleMethod): T =
    resample(Extent(0.0, 0.0, 1000.0, 1000.0), targetCols, targetRows, method)

  def resample(targetCols: Int, targetRows: Int): T =
    resample(Extent(0.0, 0.0, 1000.0, 1000.0), targetCols, targetRows, ResampleMethod.DEFAULT)
}
