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

import geotrellis.spark.io.index.geowave._
import geotrellis.spark.KeyBounds
import geotrellis.spark.SpaceTimeKey

import org.scalatest._

import jp.ne.opt.chronoscala.Imports._

import java.time.temporal.ChronoUnit.MILLIS
import java.time.{ZoneOffset, ZonedDateTime}

class GeowaveSpaceTimeKeyIndexSpec extends FunSpec with Matchers {

  val upperBound: Int = 16 // corresponds to width of 4 2^4
  val y2k = ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  describe("GeowaveSpaceTimeKeyIndex tests"){

    it("indexes col, row, and time"){
      val kb = KeyBounds(
        SpaceTimeKey(0, 0, y2k),
        SpaceTimeKey(upperBound-1, upperBound-1, y2k.plus(upperBound, MILLIS)))
      val hst = new GeowaveSpaceTimeKeyIndex(kb, 4, 4, 16, 0, GeowaveMinute())
      val keys =
        for(col <- 0 until upperBound;
             row <- 0 until upperBound;
                t <- 0 until upperBound) yield {
          hst.toIndex(SpaceTimeKey(col, row, y2k.plus(t, MILLIS)))
        }

      keys.distinct.size should be (upperBound * upperBound * upperBound)
    }

  }
}
