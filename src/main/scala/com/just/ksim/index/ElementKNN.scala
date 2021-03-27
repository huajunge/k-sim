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
  val psMaximum = Array(0, 10, 0, 1, 0, 2, 9, 3, 0, 8, 0, 5, 0, 6, 7, 4)
  val positionIndex = Array(3, 5, 7, 15, 11, 13, 14, 9, 6, 1)
  val positionDisMap = new util.HashMap[Long, (Double, Int)]()
  val positionDisMap2 = new util.HashMap[Long, Boolean]()
  val children = new java.util.ArrayList[ElementKNN](4)

  //  def getLevel(): Int = {
  //    level
  //  }

  def neededToCheck(traj: Envelope, threshold: Double): Boolean = {
    if (checked) {
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
      children.add(new ElementKNN(xmin, ymin, xCenter, yCenter, level + 1, g, pre, elementCode + 9L))
      children.add(new ElementKNN(xCenter, ymin, xmax, yCenter, level + 1, g, pre, elementCode + 9L + 1L * IS(level + 1)))
      children.add(new ElementKNN(xmin, yCenter, xCenter, ymax, level + 1, g, pre, elementCode + 9L + 2L * IS(level + 1)))
      children.add(new ElementKNN(xCenter, yCenter, xmax, ymax, level + 1, g, pre, elementCode + 9L + 3L * IS(level + 1)))
    }
  }

  def IS(i: Int): Long = {
    (39L * math.pow(4, g - i).toLong - 9L) / 3L
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

    if ((addedPositionCodes == 0x1FF && level < g) || (addedPositionCodes == 0x3FF && level == g)) {
      return new java.util.ArrayList[IndexRange]
    }
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
    var outPositions = 0

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
    //
    val thr_center_ll = new Coordinate(xCenter - threshold, yCenter - threshold)
    val thr_center_rl = new Coordinate(xCenter + threshold, yCenter - threshold)
    val thr_center_lu = new Coordinate(xCenter - threshold, yCenter + threshold)
    val thr_center_ru = new Coordinate(xCenter + threshold, yCenter + threshold)
    val thr_upperCenter_l = new Coordinate(xCenter - threshold, ymax + threshold)
    val thr_upperCenter_r = new Coordinate(xCenter + threshold, ymax + threshold)
    val thr_lowerCenter_l = new Coordinate(xCenter - threshold, ymin - threshold)
    val thr_lowerCenter_r = new Coordinate(xCenter + threshold, ymin - threshold)
    val thr_centerLeft_l = new Coordinate(xmin - threshold, yCenter - threshold)
    val thr_centerLeft_u = new Coordinate(xmin - threshold, yCenter + threshold)
    val thr_centerRight_l = new Coordinate(xmax + threshold, yCenter - threshold)
    val thr_centerRight_u = new Coordinate(xmax + threshold, yCenter + threshold)

    val thr_lowerLeft = new Coordinate(xmin - threshold, ymin - threshold)
    val thr_upperLeft = new Coordinate(xmin - threshold, ymax + threshold)
    val thr_upperRight = new Coordinate(xmax + threshold, ymax + threshold)
    val thr_lowerRight = new Coordinate(xmax + threshold, ymin - threshold)

    val results = new java.util.ArrayList[Long](8)
    var pSize = 9L
    if (level < g) {
      pSize = 8L
    }
    for (i <- 0L to pSize) {
      val sig = positionIndex(i.toInt)
      if (!((sig.toInt & outPositions) > 0) && ((addedPositionCodes & (1 << i.toInt)) == 0)) {
        var checked = true
        if ((checkedPositionCodes & (1 << i.toInt)) > 0) {
          //check = positionDisMap.get(i)._1 <= threshold
          checked = !positionDisMap2.get(i)
        }
        if (sig == 15 || !checked) {
          this.addedPositionCodes |= (1 << i.toInt)
          results.add(i + 1L)
        }
        //if (!checked) {
//        if ((checkedPositionCodes & (1 << i.toInt)) > 0) {
//          checked = positionDisMap.get(i)._1 <= threshold
//        }

        if (checked) {
          {
            var cps = new Array[Coordinate](4)
            //            sig match {
            //              case 1 =>
            //                cps = Array(lowerLeft, centerLeft, center, lowerCenter, lowerLeft)
            //              case 3 =>
            //                cps = Array(lowerLeft, centerLeft, centerRight, lowerRight, lowerLeft)
            //              case 5 =>
            //                cps = Array(lowerLeft, upperLeft, upperCenter, lowerCenter, lowerLeft)
            //              case 6 =>
            //                cps = Array(centerLeft, upperLeft, upperCenter, center, centerRight, lowerRight, lowerCenter, center, centerLeft)
            //              case 7 =>
            //                cps = Array(lowerLeft, upperLeft, upperCenter, center, centerRight, lowerRight, lowerLeft)
            //              case 9 =>
            //                cps = Array(lowerLeft, centerLeft, center, upperCenter, upperRight, centerRight, center, lowerCenter, lowerLeft)
            //              case 11 =>
            //                cps = Array(lowerLeft, centerLeft, center, upperCenter, upperRight, lowerRight, lowerLeft)
            //              case 13 =>
            //                cps = Array(lowerLeft, upperLeft, upperRight, centerRight, center, lowerCenter, lowerLeft)
            //              case 14 =>
            //                cps = Array(centerLeft, upperLeft, upperRight, lowerRight, lowerCenter, center, centerLeft)
            //              case 15 =>
            //                cps = Array(lowerLeft, upperLeft, upperRight, lowerRight, lowerLeft)
            //            }
            sig match {
              case 1 =>
                cps = Array(thr_lowerLeft, thr_centerLeft_u, thr_center_ru, thr_lowerCenter_r, thr_lowerLeft)
              case 3 =>
                cps = Array(thr_lowerLeft, thr_centerLeft_u, thr_centerRight_u, thr_lowerRight, thr_lowerLeft)
              case 5 =>
                cps = Array(thr_lowerLeft, thr_upperLeft, thr_upperCenter_r, thr_lowerCenter_r, thr_lowerLeft)
              case 6 =>
                cps = Array(thr_centerLeft_l, thr_upperLeft, thr_upperCenter_r, thr_center_ru, thr_centerRight_u, thr_lowerRight, thr_lowerCenter_l, thr_center_ll, thr_centerLeft_l)
              case 7 =>
                cps = Array(thr_lowerLeft, thr_upperLeft, thr_upperCenter_r, thr_center_ru, thr_centerRight_u, thr_lowerRight, thr_lowerLeft)
              case 9 =>
                cps = Array(thr_lowerLeft, thr_centerLeft_u, thr_center_lu, thr_upperCenter_l, thr_upperRight, thr_centerRight_l, thr_center_rl, thr_lowerCenter_r, thr_lowerLeft)
              case 11 =>
                cps = Array(thr_lowerLeft, thr_centerLeft_u, thr_center_lu, thr_upperCenter_l, thr_upperRight, thr_lowerRight, thr_lowerLeft)
              case 13 =>
                cps = Array(thr_lowerLeft, thr_upperLeft, thr_upperRight, thr_centerRight_l, thr_center_rl, thr_lowerCenter_r, thr_lowerLeft)
              case 14 =>
                cps = Array(thr_centerLeft_l, thr_upperLeft, thr_upperRight, thr_lowerRight, thr_lowerCenter_l, thr_center_ll, thr_centerLeft_l)
              case 15 =>
                cps = Array(thr_lowerLeft, thr_upperLeft, thr_upperRight, thr_lowerRight, thr_lowerLeft)
            }
            //var st = System.currentTimeMillis()
            var dis = (0.0, true, 1)
            var disTmp = 0.0

            var contained = true
            contained = disOfPosAndTraj(cps, threshold, 1)._2
            positionDisMap2.put(i, contained)
            if (contained) {
              this.addedPositionCodes |= (1 << i.toInt)
              results.add(i + 1L)
              //println(s"$sig,${i + 1L}")
            }

            //            if ((checkedPositionCodes & (1 << i.toInt)) > 0) {
            //              dis = disOfPosAndTraj(cps, threshold, positionDisMap.get(i)._2)
            //              disTmp = positionDisMap.get(i)._1
            //              //dis = disOfPosAndTraj(cps, threshold, 0)
            //            } else {
            //              dis = disOfPosAndTraj(cps, threshold, 1)
            //            }
            //            positionDisMap.put(i, (Math.max(dis._1, disTmp), dis._3))
            //            checkedPositionCodes |= (1 << i.toInt)
            //
            //            if (dis._2) {
            //              this.addedPositionCodes |= (1 << i.toInt)
            //              results.add(i + 1L)
            //              //println(s"$sig,${i + 1L}")
            //            }
          }
          //var et = System.currentTimeMillis()
          //println(s"checking----:${et-st}")
        }
      }
    }

    def disOfPosAndTraj(coordinats: Array[Coordinate], threshold: Double, startIndex: Int): (Double, Boolean, Int) = {
      val line = new LinearRing(coordinats, pre, 4326)
      val polygon = new Polygon(line, null, pre, 4326)
            if (polygon.contains(traj.getMultiPoint)) {
              (0.0, true, 0)
            } else {
              (0.0, false, 0)
            }

//      var maxDis = 0.0
      //      val sDis = polygon.distance(spoint)
      //      val eDis = polygon.distance(epoint)
      //      maxDis = Math.max(sDis, eDis)
      //      var index = startIndex
      //      if (sDis <= threshold && eDis <= threshold) {
      //        for (i <- startIndex until(traj.getNumGeometries - 1, 1)) {
      //          index = i
      //          val dis = polygon.distance(traj.getGeometryN(i))
      //          if (maxDis < dis) {
      //            maxDis = dis
      //          }
      //          if (dis > threshold) {
      //            return (maxDis, false, index)
      //          }
      //        }
      //        return (maxDis, true, index)
      //      }
      //
      //      (maxDis, false, index)
    }

    results.asScala.map(v => {
      //println(s"$level,$v,$elementCode")
      IndexRange(v + elementCode - 10L, v + elementCode - 10L, contained = false)
    }
    ).asJava
  }

  def dis(env: Envelope, geo: Geometry): Double = {
    val cps = Array(new Coordinate(env.getMinX, env.getMinY),
      new Coordinate(env.getMinX, env.getMaxY),
      new Coordinate(env.getMaxX, env.getMaxY),
      new Coordinate(env.getMaxX, env.getMinY), new Coordinate(env.getMinX, env.getMinY))
    val line = new LinearRing(cps, pre, 4326)
    val polygon = new Polygon(line, null, pre, 4326)
    polygon.distance(geo)
  }
}

