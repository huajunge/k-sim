package com.just.ksim.urbancomputing.index

import com.just.ksim.urbancomputing.entity.Trajectory
import org.locationtech.jts.geom.{Coordinate, LinearRing, Polygon, PrecisionModel}

class IndexSpace(code: Long, shape: Array[Coordinate], sig: Int, pre: PrecisionModel, level: Int) {
  val shapeGeo = new Polygon(new LinearRing(shape, pre, 4326), null, pre, 4326)

  def getCode(): Long = {
    code
  }

  def getLevel(): Int = {
    level
  }

  def getShape(): Array[Coordinate] = {
    shape
  }

  def getSig(): Int = {
    sig
  }

  import scala.collection.JavaConverters._

  def dis(tra: Trajectory): Double = {
    tra.getDPFeature.getIndexes.asScala.map(v1 => {
      tra.getGeometryN(v1).distance(shapeGeo)
    }).max(new Ordering[Double] {
      override def compare(x: Double, y: Double): Int = {
        java.lang.Double.compare(x, y)
      }
    })
  }
}
