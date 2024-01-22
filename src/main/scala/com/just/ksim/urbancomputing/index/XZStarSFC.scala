package com.just.ksim.urbancomputing.index

import com.just.ksim.urbancomputing.entity.Trajectory
import org.locationtech.jts.geom._

import java.util.Comparator

class XZStarSFC(g: Short, xBounds: (Double, Double), yBounds: (Double, Double), beta: Int) extends XZSFC(g, xBounds, yBounds, beta) with Serializable {
  def index(geometry: Geometry, lenient: Boolean = false): Long = {
    indexPocLength(geometry, lenient)._1
  }

  def indexLength(geometry: Geometry, lenient: Boolean = false): String = {
    var s = ""
    for (_ <- 0 to indexPocLength(geometry, lenient)._3) {
      s = s + "1"
    }
    s
  }

  def indexPocLength(geometry: Geometry, lenient: Boolean = false): (Long, Long, Int) = {
    //geometry.getBoundary
    val mbr = geometry.getEnvelopeInternal
    val (nxmin, nymin, nxmax, nymax) = normalize(mbr.getMinX, mbr.getMinY, mbr.getMaxX, mbr.getMaxY, lenient)
    val maxDim = math.max(nxmax - nxmin, nymax - nymin)
    val l1 = math.floor(math.log(maxDim) / XZSFC.LogPointFive).toInt

    // the length will either be (l1) or (l1 + 1)
    val length = if (l1 >= g) {
      g
    } else {
      val w2 = math.pow(0.5, l1 + 1) // width of an element at resolution l2 (l1 + 1)

      // predicate for checking how many axis the polygon intersects
      // math.floor(min / w2) * w2 == start of cell containing min
      def predicate(min: Double, max: Double): Boolean = max <= (math.floor(min / w2) * w2) + (2 * w2)

      if (predicate(nxmin, nxmax) && predicate(nymin, nymax)) l1 + 1 else l1
    }
    val w = math.pow(0.5, length)
    val x = math.floor(nxmin / w) * w
    val y = math.floor(nymin / w) * w

    val pc = signature(x * xSize + xLo, y * ySize + yLo, (x + 2 * w) * xSize + xLo, (y + 2 * w) * ySize + yLo, geometry)

    (sequenceCode(nxmin, nymin, length, pc), pc, length)
  }

  def resolution(length: Double, dir: Int): Int = {
    //geometry.getBoundary
    if (length <= 0.0) {
      return g
    }
    var maxDim = 0.0
    if (dir == 0) {
      maxDim = length / xSize
    } else {
      maxDim = length / ySize
    }
    val l1 = math.floor(math.log(maxDim) / XZSFC.LogPointFive).toInt
    if (l1 >= g) {
      return g
    }
    l1
  }

  def sequenceCode(x: Double, y: Double, length: Int, posCode: Long): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = 0L

    def IS(i: Int): Long = {
      (39L * math.pow(4, g - i).toLong - 9L) / 3L
    }

    var i = 1
    while (i <= length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) => cs += 9L; xmax = xCenter; ymax = yCenter
        case (false, true) => cs += 9L + 1L * IS(i); xmin = xCenter; ymax = yCenter
        case (true, false) => cs += 9L + 2L * IS(i); xmax = xCenter; ymin = yCenter
        case (false, false) => cs += 9L + 3L * IS(i); xmin = xCenter; ymin = yCenter
      }
      i += 1
    }
    cs - 10L + posCode
  }

  def localFilter(searTraj: Trajectory, is: (IndexSpace, Double), currentThreshold: Double): Boolean = {
    false
  }

  //  def rangesForKnn(searTraj: Trajectory, k: Int): java.util.List[IndexRange] = {
  //    import scala.util.control.Breaks._
  //    val currentThreshold = Double.MaxValue
  //    val ranges = new java.util.ArrayList[IndexRange](100)
  //    val elements: MinMaxPriorityQueue[(EnlargedElement, Double)] =
  //      MinMaxPriorityQueue.orderedBy(new elementComp()).maximumSize(k).create
  //    val indexSpaces: MinMaxPriorityQueue[(IndexSpace, Double)] =
  //      MinMaxPriorityQueue.orderedBy(new ISComp()).maximumSize(k).create
  //
  //    elements.add(new EnlargedElement(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel, 0L), 0.0)
  //    while (!elements.isEmpty) {
  //      val e = elements.poll()
  //      if (!indexSpaces.isEmpty && e._2 > indexSpaces.peek()._2) {
  //        while (!indexSpaces.isEmpty) {
  //          if (indexSpaces.peek()._2 > e._2) {
  //            break
  //          }
  //          val is = indexSpaces.poll()
  //          if (is._2 >= currentThreshold) {
  //            return null
  //          }
  //          if (!localFilter(searTraj, is, currentThreshold)) {
  //            ranges.add(IndexRange(is._1.getCode(), is._1.getCode(), contained = true))
  //          }
  //        }
  //      }
  //    }
  //    null
  //  }

  class ISComp extends Comparator[(IndexSpace, Double)] {
    override def compare(o1: (IndexSpace, Double), o2: (IndexSpace, Double)): Int = {
      java.lang.Double.compare(o1._2, o2._2)
    }
  }

  class elementComp extends Comparator[(EnlargedElement, Double)] {
    override def compare(o1: (EnlargedElement, Double), o2: (EnlargedElement, Double)): Int = {
      java.lang.Double.compare(o1._2, o2._2)
    }
  }
}

object XZStarSFC extends Serializable {
  // the initial level of quads
  private val cache = new java.util.concurrent.ConcurrentHashMap[(Short, Int), XZStarSFC]()

  def apply(g: Short, beta: Int): XZStarSFC = {
    var sfc = cache.get((g, beta))
    if (sfc == null) {
      sfc = new XZStarSFC(g, (-180.0, 180.0), (-90.0, 90.0), beta)
      cache.put((g, beta), sfc)
    }
    sfc
  }
}