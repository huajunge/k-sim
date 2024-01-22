package com.just.ksim.urbancomputing.index

import com.just.ksim.urbancomputing.entity.Trajectory
import org.locationtech.jts.geom._

import java.util

class EnlargedElement(val xmin: Double, val ymin: Double, val xmax: Double, val ymax: Double, val level: Int, val g: Int, val pre: PrecisionModel, val elementCode: Long) {
  val xLength = xmax - xmin
  val yLength = ymax - ymin
  val psMaximum = Array(0, 10, 0, 1, 0, 2, 9, 3, 0, 8, 0, 5, 0, 6, 7, 4)
  val positionIndex = Array(3, 5, 7, 15, 11, 13, 14, 9, 6, 1)
  val positionDisMap = new util.HashMap[Long, (Double, Int)]()
  val positionDisMap2 = new util.HashMap[Long, Boolean]()
  val children = new java.util.ArrayList[EnlargedElement](4)
  val xeMax = this.xmax + xLength
  val yeMax = this.ymax + xLength
  val xCenter = this.xmax
  val yCenter = this.ymax

  val center = new Coordinate(xCenter, yCenter)
  val upperCenter = new Coordinate(xCenter, yeMax)
  val lowerCenter = new Coordinate(xCenter, ymin)
  val centerLeft = new Coordinate(xmin, yCenter)
  val centerRight = new Coordinate(xeMax, yCenter)

  val lowerLeft = new Coordinate(xmin, ymin)
  val upperLeft = new Coordinate(xmin, yeMax)
  val upperRight = new Coordinate(xeMax, yeMax)
  val lowerRight = new Coordinate(xeMax, ymin)

  //val shape = new Envelope(xmin, xeMax, ymin, yeMax)
  val shape = new Polygon(new LinearRing(Array(lowerLeft, upperLeft, upperRight, lowerRight, lowerLeft), pre, 4326), null, pre, 4326)
  val cellShape = new Polygon(new LinearRing(Array(lowerLeft, centerLeft, center, lowerCenter, lowerLeft), pre, 4326), null, pre, 4326)

  //  def getLevel(): Int = {
  //    level
  //  }

  def split(): Unit = {
    if (children.isEmpty) {
      val xCenter = (xmax + xmin) / 2.0
      val yCenter = (ymax + ymin) / 2.0
      children.add(new EnlargedElement(xmin, ymin, xCenter, yCenter, level + 1, g, pre, elementCode + 9L))
      children.add(new EnlargedElement(xCenter, ymin, xmax, yCenter, level + 1, g, pre, elementCode + 9L + 1L * IS(level + 1)))
      children.add(new EnlargedElement(xmin, yCenter, xCenter, ymax, level + 1, g, pre, elementCode + 9L + 2L * IS(level + 1)))
      children.add(new EnlargedElement(xCenter, yCenter, xmax, ymax, level + 1, g, pre, elementCode + 9L + 3L * IS(level + 1)))
    }
  }

  def IS(i: Int): Long = {
    (39L * math.pow(4, g - i).toLong - 9L) / 3L
  }

  def search(root: EnlargedElement, x: Double, y: Double, l: Int): EnlargedElement = {
    var i = root.level
    var currentElement = root
    while (i < l) {
      val xCenter = (currentElement.xmin + currentElement.xmax) / 2.0
      val yCenter = (currentElement.ymin + currentElement.ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) => currentElement = currentElement.getChildren.get(0)
        case (false, true) => currentElement = currentElement.getChildren.get(1)
        case (true, false) => currentElement = currentElement.getChildren.get(2)
        case (false, false) => currentElement = currentElement.getChildren.get(3)
      }
      i += 1
    }
    currentElement
  }

  def getChildren: util.ArrayList[EnlargedElement] = {
    if (children.isEmpty) {
      split()
    }
    children
  }

  def indexSpaces: util.ArrayList[IndexSpace] = {
    val ISpaces = new java.util.ArrayList[IndexSpace](16)
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(1).toLong, Array(lowerLeft, centerLeft, center, lowerCenter, lowerLeft), 1, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(3).toLong, Array(lowerLeft, centerLeft, centerRight, lowerRight, lowerLeft), 3, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(5).toLong, Array(lowerLeft, upperLeft, upperCenter, lowerCenter, lowerLeft), 5, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(6).toLong, Array(centerLeft, upperLeft, upperCenter, center, centerRight, lowerRight, lowerCenter, center, centerLeft), 6, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(7).toLong, Array(lowerLeft, upperLeft, upperCenter, center, centerRight, lowerRight, lowerLeft), 7, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(9).toLong, Array(lowerLeft, centerLeft, center, upperCenter, upperRight, centerRight, center, lowerCenter, lowerLeft), 9, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(11).toLong, Array(lowerLeft, centerLeft, center, upperCenter, upperRight, lowerRight, lowerLeft), 11, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(13).toLong, Array(lowerLeft, upperLeft, upperRight, centerRight, center, lowerCenter, lowerLeft), 13, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(14).toLong, Array(centerLeft, upperLeft, upperRight, lowerRight, lowerCenter, center, centerLeft), 14, pre, level))
    ISpaces.add(new IndexSpace(this.elementCode - 10L + psMaximum(15).toLong, Array(lowerLeft, upperLeft, upperRight, lowerRight, lowerLeft), 15, pre, level))
    ISpaces
  }

  import scala.collection.JavaConverters._

  def dis(tra: Trajectory): Double = {
    tra.getDPFeature.getIndexes.asScala.map(v1 => {
      tra.getGeometryN(v1).distance(shape)
    }).max(new Ordering[Double] {
      override def compare(x: Double, y: Double): Int = {
        java.lang.Double.compare(x, y)
      }
    })
  }

  def disWithQuads(tra: Trajectory): Double = {
    val dis1 = tra.getDPFeature.getIndexes.asScala.map(v1 => {
      tra.getGeometryN(v1).distance(shape)
    }).max(new Ordering[Double] {
      override def compare(x: Double, y: Double): Int = {
        java.lang.Double.compare(x, y)
      }
    })
    var min1 = Double.MaxValue
    var min2 = Double.MaxValue
    for (elem <- disOfQuads(tra)) {
      if (elem < min1) {
        min2 = min1
        min1 = elem
      } else if (elem < min2) {
        min2 = elem
      }
    }
    java.lang.Double.max(dis1, min2)
  }

  def cellDisToTraj(tra: Trajectory): Double = {
    tra.getEnv.getGeometryN(0).distance(cellShape)
  }

  def disOfQuads(traj: Trajectory): Array[Double] = {

    //Array(lowerLeft, centerLeft, center, lowerCenter, lowerLeft))
    val c0 = new Polygon(new LinearRing(Array(lowerLeft, centerLeft, center, lowerCenter, lowerLeft), pre, 4326), null, pre, 4326)
    val c1 = new Polygon(new LinearRing(Array(lowerCenter, center, centerRight, lowerRight, lowerCenter), pre, 4326), null, pre, 4326)
    val c2 = new Polygon(new LinearRing(Array(centerLeft, center, upperCenter, upperLeft, centerLeft), pre, 4326), null, pre, 4326)
    val c3 = new Polygon(new LinearRing(Array(center, centerRight, upperRight, upperRight, center), pre, 4326), null, pre, 4326)
    var d0 = 0.0
    var d1 = 0.0
    var d2 = 0.0
    var d3 = 0.0
    d0 = c0.distance(traj.getEnv)
    d1 = c1.distance(traj.getEnv)
    d2 = c2.distance(traj.getEnv)
    d3 = c3.distance(traj.getEnv)
    Array(d0, d1, d2, d3)
  }

  def invalidQuads(traj: Trajectory, threshold: Double): (Int, Array[Double]) = {
    val d = disOfQuads(traj)
    var sig = 0
    for (i <- 0 until 4) {
      if (d(i) >= threshold) {
        sig |= (1 << i)
      }
    }
    (sig, d)
  }
}

