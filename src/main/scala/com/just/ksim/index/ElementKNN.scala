package com.just.ksim.index

import com.just.ksim.entity.Trajectory
import org.locationtech.jts.geom._
import org.locationtech.sfcurve.IndexRange

import java.util
import scala.collection.JavaConverters._

class ElementKNN(val xmin: Double, val ymin: Double, val xmax: Double, val ymax: Double, val level: Int, val g: Int, val pre: PrecisionModel, val elementCode: Long) {
  var checked = false
  var addedPositionCodes = 0
  var checkedPositionCodes = 0
  //private var distance = 0.0
  //private var elementCode = -1L
  private val disOfFourSubElements = new java.util.ArrayList[Double](4)
  val xLength = xmax - xmin
  val yLength = ymax - ymin
  val psMaximum = Array(0, 8, 0, 1, 0, 2, 0, 3, 0, 0, 0, 5, 0, 6, 7, 4)
  val positionIndex = Array(3, 5, 7, 15, 11, 13, 14, 1)
  val positionDisMap = new util.HashMap[Long, Double]()
  val children = new java.util.ArrayList[ElementKNN](4)

  //  def getLevel(): Int = {
  //    level
  //  }

  def neededToCheck(traj: Envelope, threshold: Double): Boolean = {
    if ((addedPositionCodes == 0x7F && level < g) || (addedPositionCodes == 0xFF && level == g)) {
      return false
    }
    if (checked) {
      //println("checked:"+checked)
      return checked
    }
    val enlElement = new Envelope(xmin, xmax + xLength, ymin, ymax + yLength)
    enlElement.expandBy(threshold)
    checked = enlElement.contains(traj)
    checked
  }

  def split(): Unit = {
    if (children.isEmpty) {
      val xCenter = (xmax + xmin) / 2.0
      val yCenter = (ymax + ymin) / 2.0
      children.add(new ElementKNN(xmin, ymin, xCenter, yCenter, level + 1, g, pre, elementCode + 7L))
      children.add(new ElementKNN(xCenter, ymin, xmax, yCenter, level + 1, g, pre, elementCode + 7L + 1L * IS(level + 1)))
      children.add(new ElementKNN(xmin, yCenter, xCenter, ymax, level + 1, g, pre, elementCode + 7L + 2L * IS(level + 1)))
      children.add(new ElementKNN(xCenter, yCenter, xmax, ymax, level + 1, g, pre, elementCode + 7L + 3L * IS(level + 1)))
    }
  }

  def IS(i: Int): Long = {
    (31L * math.pow(4, g - i).toLong - 7L) / 3L
  }

  def search(root: ElementKNN, x: Double, y: Double, l: Int): ElementKNN = {
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

  def getChildren: util.ArrayList[ElementKNN] = {
    if (children.isEmpty) {
      split()
    }
    children
  }

  def checkPositionCode(traj: Trajectory, threshold: Double, spoint: Geometry, epoint: Geometry): util.List[IndexRange] = {
    val xeMax = this.xmax + xLength
    val yeMax = this.ymax + xLength
    val xCenter = this.xmax
    val yCenter = this.ymax
    val ymax = yeMax
    val xmax = xeMax
    var d0 = 0.0
    var d1 = 0.0
    var d2 = 0.0
    var d3 = 0.0
    if (disOfFourSubElements.isEmpty) {
      val c0 = new Envelope(xmin, xCenter, ymin, yCenter)
      val c1 = new Envelope(xCenter, xmax, ymin, yCenter)
      val c2 = new Envelope(xmin, xCenter, yCenter, ymax)
      val c3 = new Envelope(xCenter, xmax, yCenter, ymax)
      d0 = dis(c0, traj.getMultiPoint)
      d1 = dis(c1, traj.getMultiPoint)
      d2 = dis(c2, traj.getMultiPoint)
      d3 = dis(c3, traj.getMultiPoint)
      disOfFourSubElements.add(d0)
      disOfFourSubElements.add(d1)
      disOfFourSubElements.add(d2)
      disOfFourSubElements.add(d3)
    } else {
      //println("disOfFourSubElements:")
      d0 = disOfFourSubElements.get(0)
      d1 = disOfFourSubElements.get(1)
      d2 = disOfFourSubElements.get(2)
      d3 = disOfFourSubElements.get(3)
    }
    var outPositions = 0
    if (d0 > threshold) {
      outPositions |= 1
    }
    if (d1 > threshold) {
      outPositions |= 1 << 1
    }
    if (d2 > threshold) {
      outPositions |= 1 << 2
    }
    if (d3 > threshold) {
      outPositions |= 1 << 3
    }

    //println(outPositions)
    val center = new Coordinate(xCenter, yCenter)
    val upperCenter = new Coordinate(xCenter, ymax)
    val lowerCenter = new Coordinate(xCenter, ymin)
    val centerLeft = new Coordinate(xmin, yCenter)
    val centerRight = new Coordinate(xmax, yCenter)

    val lowerLeft = new Coordinate(xmin, ymin)
    val upperLeft = new Coordinate(xmin, ymax)
    val upperRight = new Coordinate(xmax, ymax)
    val lowerRight = new Coordinate(xmax, ymin)

    val results = new java.util.ArrayList[Long](8)
    var pSize = 7L
    if (level < g) {
      pSize = 6L
    }
    for (i <- 0L to pSize) {
      val sig = positionIndex(i.toInt)
      if (!((sig.toInt & outPositions) > 0) && ((addedPositionCodes & (1 << i.toInt)) == 0)) {
        var check = true
        if ((checkedPositionCodes & (1 << i.toInt)) > 0) {
          check = positionDisMap.get(i) < threshold
        }
        if (check) {
          if (sig == 15) {
            results.add(i + 1L)
            positionDisMap.put(i, 0.0)
            checkedPositionCodes |= (1 << i.toInt)
          } else {
            var cps = new Array[Coordinate](4)
            sig match {
              case 1 =>
                cps = Array(lowerLeft, centerLeft, center, lowerCenter, lowerLeft)
              case 3 =>
                cps = Array(lowerLeft, centerLeft, centerRight, lowerRight, lowerLeft)
              case 5 =>
                cps = Array(lowerLeft, upperLeft, upperCenter, lowerCenter, lowerLeft)
              case 7 =>
                cps = Array(lowerLeft, upperLeft, upperCenter, center, centerRight, lowerRight, lowerLeft)
              case 11 =>
                cps = Array(lowerLeft, centerLeft, center, upperCenter, upperRight, lowerRight, lowerLeft)
              case 13 =>
                cps = Array(lowerLeft, upperLeft, upperRight, centerRight, center, lowerCenter, lowerLeft)
              case 14 =>
                cps = Array(centerLeft, upperLeft, upperRight, lowerRight, lowerCenter, center, centerLeft)
            }

            //var st = System.currentTimeMillis()
            val dis = disOfPosAndTraj(cps, threshold)
            positionDisMap.put(i, dis._1)
            checkedPositionCodes |= (1 << i.toInt)

            if (dis._2) {
              addedPositionCodes |= (1 << i.toInt)
              results.add(i + 1L)
              //println(s"$sig,${i + 1L}")
            }
          }
          //var et = System.currentTimeMillis()
          //println(s"checking----:${et-st}")
        }
      }
    }

    def disOfPosAndTraj(coordinats: Array[Coordinate], threshold: Double): (Double, Boolean) = {
      val line = new LinearRing(coordinats, pre, 4326)
      val polygon = new Polygon(line, null, pre, 4326)
      var maxDis = 0.0
      val sDis = polygon.distance(spoint)
      val eDis = polygon.distance(epoint)
      maxDis = Math.max(sDis, eDis)
      if (sDis <= threshold && eDis <= threshold) {
        for (i <- 1 until traj.getNumGeometries - 1) {
          val dis = polygon.distance(traj.getGeometryN(i))
          if (maxDis < dis) {
            maxDis = dis
          }
          if (dis > threshold) {
            return (maxDis, false)
          }
        }
        return (maxDis, true)
      }
      (maxDis, false)
    }

    results.asScala.map(v =>
      IndexRange(v + elementCode - 7L, v + elementCode - 7L, contained = false)
    ).asJava
  }

  def dis(env: Envelope, geo: Geometry): Double = {
    val cps = Array(new Coordinate(env.getMinX, env.getMinY), new Coordinate(env.getMinX, env.getMinY),
      new Coordinate(env.getMinX, env.getMaxY),
      new Coordinate(env.getMaxX, env.getMaxY),
      new Coordinate(env.getMaxX, env.getMinY), new Coordinate(env.getMinX, env.getMinY))
    val line = new LinearRing(cps, pre, 4326)
    val polygon = new Polygon(line, null, pre, 4326)
    polygon.distance(geo)
  }
}

