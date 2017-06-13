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

import org.scalatest._
import scala.collection.immutable.TreeSet
import geotrellis.spark.SpatialKey

class GeowaveSpatialKeyIndexSpec extends FunSpec with Matchers{

  val upperBound: Int = 64

  describe("GeowaveSpatialKeyIndex tests"){

    it("Generates a Long index given a SpatialKey"){
      val geowave = GeowaveSpatialKeyIndex(SpatialKey(0,0), SpatialKey(upperBound,upperBound), 7)

      val keys =
        for(col <- 0 until upperBound;
           row <- 0 until upperBound) yield {
          geowave.toIndex(SpatialKey(col,row))
        }

      keys.distinct.size should be (upperBound * upperBound)
    }

    it("generates hand indexes you can hand check 2x2"){
     val geowave = GeowaveSpatialKeyIndex(SpatialKey(0,0), SpatialKey(1,1), 1)
     //right oriented
     geowave.toIndex(SpatialKey(0,0)) should be (0)
     geowave.toIndex(SpatialKey(0,1)) should be (1)
     geowave.toIndex(SpatialKey(1,1)) should be (2)
     geowave.toIndex(SpatialKey(1,0)) should be (3)
    }

    it("generates hand indexes you can hand check 4x4"){
     val geowave = GeowaveSpatialKeyIndex(SpatialKey(0,0), SpatialKey(3, 3), 2)
     val grid = List[SpatialKey]( SpatialKey(0,0), SpatialKey(1,0), SpatialKey(1,1), SpatialKey(0,1),
                                  SpatialKey(0,2), SpatialKey(0,3), SpatialKey(1,3), SpatialKey(1,2),
                                  SpatialKey(2,2), SpatialKey(2,3), SpatialKey(3,3), SpatialKey(3,2),
                                  SpatialKey(3,1), SpatialKey(2,1), SpatialKey(2,0), SpatialKey(3,0))
     for(i <- 0 to 15){
       geowave.toIndex(grid(i)) should be (i)
     }
    }

    it("Generates a Seq[(Long,Long)] given a key range (SpatialKey,SpatialKey)"){
        //hand re-checked examples
        val geowave = GeowaveSpatialKeyIndex(SpatialKey(0,0), SpatialKey(3, 3), 2)

        // single point, min corner
        var idx = geowave.indexRanges((SpatialKey(0,0), SpatialKey(0,0)))
        idx.length should be (1)
        idx.toSet should be (Set(0->0))

        // single point, max corner
        idx = geowave.indexRanges((SpatialKey(3,3), SpatialKey(3,3)))
        idx.length should be (1)
        idx.toSet should be (Set(10->10)) // aha, not 15 as you might think!

        //square grids
        idx = geowave.indexRanges((SpatialKey(0,0), SpatialKey(1,1)))
        idx.length should be (1)
        idx.toSet should be (Set(0->3))

        idx = geowave.indexRanges((SpatialKey(0,0), SpatialKey(3,3)))
        idx.length should be (1)
        idx.toSet should be (Set(0->15))

        //check some subgrid
        idx = geowave.indexRanges((SpatialKey(1,0), SpatialKey(3,2)))
        idx.length should be (3)
        idx.toSet should be (Set(1->2, 7->8, 11->15))
     }
  }
}
