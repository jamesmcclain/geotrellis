/*
 * Copyright (c) 2014 Azavea.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.op.local

import geotrellis._
import geotrellis.source._

/**
 * Determines if values are greater than or equal to other values. Sets to 1 if true, else 0.
 */
object GreaterOrEqual extends LocalRasterBinaryOp {
  def combine(z1:Int,z2:Int) =
    if(z1 >= z2) 1 else 0

  def combine(z1:Double,z2:Double):Double =
    if(isNoData(z1)) { 0 }
    else {
      if(isNoData(z2)) { 0 }
      else { 
        if(z1 >= z2) 1 
        else 0 
      }
    }
}

trait GreaterOrEqualOpMethods[+Repr <: RasterSource] { self: Repr =>
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * integer, else 0.
   */
  def localGreaterOrEqual(i: Int): RasterSource = self.mapOp(GreaterOrEqual(_, i))
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * integer, else 0.
   */
  def localGreaterOrEqualRightAssociative(i: Int): RasterSource = self.mapOp(GreaterOrEqual(i, _))
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
    * integer, else 0.
   */
  def >=(i:Int): RasterSource = localGreaterOrEqual(i)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * integer, else 0.
   */
  def >=:(i:Int): RasterSource = localGreaterOrEqualRightAssociative(i)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * double, else 0.
   */
  def localGreaterOrEqual(d: Double): RasterSource = self.mapOp(GreaterOrEqual(_, d))
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * double, else 0.
   */
  def localGreaterOrEqualRightAssociative(d: Double): RasterSource = self.mapOp(GreaterOrEqual(d, _))
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * double, else 0.
   */
  def >=(d:Double): RasterSource = localGreaterOrEqualRightAssociative(d)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * double, else 0.
   */
  def >=:(d:Double): RasterSource = localGreaterOrEqual(d)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell valued of the rasters are greater than or equal to the next raster, else 0.
   */
  def localGreaterOrEqual(rs:RasterSource): RasterSource = self.combineOp(rs)(GreaterOrEqual(_,_))
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell valued of the rasters are greater than or equal to the next raster, else 0.
   */
  def >=(rs:RasterSource): RasterSource = localGreaterOrEqual(rs)
}

trait GreaterOrEqualMethods { self: Raster =>
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * integer, else 0.
   */
  def localGreaterOrEqual(i: Int): Raster = GreaterOrEqual(self, i)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * integer, else 0.
   */
  def localGreaterOrEqualRightAssociative(i: Int): Raster = GreaterOrEqual(i, self)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
    * integer, else 0.
   */
  def >=(i:Int): Raster = localGreaterOrEqual(i)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * integer, else 0.
   */
  def >=:(i:Int): Raster = localGreaterOrEqualRightAssociative(i)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * double, else 0.
   */
  def localGreaterOrEqual(d: Double): Raster = GreaterOrEqual(self, d)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * double, else 0.
   */
  def localGreaterOrEqualRightAssociative(d: Double): Raster = GreaterOrEqual(d, self)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * double, else 0.
   */
  def >=(d:Double): Raster = localGreaterOrEqual(d)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell value of the input raster is greater than or equal to the input
   * double, else 0.
   */
  def >=:(d:Double): Raster = localGreaterOrEqualRightAssociative(d)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell valued of the rasters are greater than or equal to the next raster, else 0.
   */
  def localGreaterOrEqual(r:Raster): Raster = GreaterOrEqual(self,r)
  /**
   * Returns a Raster with data of TypeBit, where cell values equal 1 if
   * the corresponding cell valued of the rasters are greater than or equal to the next raster, else 0.
   */
  def >=(r:Raster): Raster = localGreaterOrEqual(r)
}