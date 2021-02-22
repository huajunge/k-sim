package com.just.ksim.index

import com.just.ksim.entity.Trajectory
import com.just.ksim.index.XZStarSFC.{LevelOneElements, LevelTerminator, XElement}
import org.locationtech.jts.geom._

class XZStarSFC(g: Short, xBounds: (Double, Double), yBounds: (Double, Double)) {
  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2

  private val xSize = xHi - xLo
  private val ySize = yHi - yLo

  def index(geometry: Geometry, lenient: Boolean = false): Long = {
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
    val x = math.floor(nxmin / w) * w * xSize + xLo
    val y = math.floor(nymin / w) * w * ySize + yLo
    val posCode = positionCode(geometry, length, w, x, y)
    sequenceCode(nxmin, nymin, length, posCode)
  }

  def positionCode(geometry: Geometry, length: Int, w: Double, x: Double, y: Double): Long = {
    0L
  }

  def sequenceCode(x: Double, y: Double, length: Int, posCode: Long): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = 0L

    def IS(i: Int): Long = {
      (31L * math.pow(4, g - i).toLong - 7L) / 3L
    }

    var i = 1
    while (i <= length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) => cs += 7L; xmax = xCenter; ymax = yCenter
        case (false, true) => cs += 7L + 1L * IS(i); xmin = xCenter; ymax = yCenter
        case (true, false) => cs += 7L + 2L * IS(i); xmax = xCenter; ymin = yCenter
        case (false, false) => cs += 7L + 3L * IS(i); xmin = xCenter; ymin = yCenter
      }
      i += 1
    }
    cs - 7L + posCode
  }

  /**
   * Normalize user space values to [0,1]
   *
   * @param xmin    min x value in user space
   * @param ymin    min y value in user space
   * @param xmax    max x value in user space, must be >= xmin
   * @param ymax    max y value in user space, must be >= ymin
   * @param lenient standardize boundaries to valid values, or raise an exception
   * @return
   */
  def normalize(xmin: Double,
                ymin: Double,
                xmax: Double,
                ymax: Double,
                lenient: Boolean): (Double, Double, Double, Double) = {
    require(xmin <= xmax && ymin <= ymax, s"Bounds must be ordered: [$xmin $xmax] [$ymin $ymax]")

    try {
      require(xmin >= xLo && xmax <= xHi && ymin >= yLo && ymax <= yHi,
        s"Values out of bounds ([$xLo $xHi] [$yLo $yHi]): [$xmin $xmax] [$ymin $ymax]")

      val nxmin = (xmin - xLo) / xSize
      val nymin = (ymin - yLo) / ySize
      val nxmax = (xmax - xLo) / xSize
      val nymax = (ymax - yLo) / ySize

      (nxmin, nymin, nxmax, nymax)
    } catch {
      case _: IllegalArgumentException if lenient =>

        val bxmin = if (xmin < xLo) {
          xLo
        } else if (xmin > xHi) {
          xHi
        } else {
          xmin
        }
        val bymin = if (ymin < yLo) {
          yLo
        } else if (ymin > yHi) {
          yHi
        } else {
          ymin
        }
        val bxmax = if (xmax < xLo) {
          xLo
        } else if (xmax > xHi) {
          xHi
        } else {
          xmax
        }
        val bymax = if (ymax < yLo) {
          yLo
        } else if (ymax > yHi) {
          yHi
        } else {
          ymax
        }

        val nxmin = (bxmin - xLo) / xSize
        val nymin = (bymin - yLo) / ySize
        val nxmax = (bxmax - xLo) / xSize
        val nymax = (bymax - yLo) / ySize

        (nxmin, nymin, nxmax, nymax)
    }
  }


  def simRange(searTraj: Trajectory, threshold: Double): Unit = {
    val ranges = new java.util.ArrayList[Long](100)
    val remaining = new java.util.ArrayDeque[XElement](100)
    // initial level
    LevelOneElements.foreach(remaining.add)
    remaining.add(LevelTerminator)
    var level: Short = 1
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next.eq(LevelTerminator)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty && level < g) {
          level = (level + 1).toShort
          remaining.add(LevelTerminator)
        }
      } else {
        checkValue(next, level)
      }
    }
    val buffer = searTraj.buffer(threshold)
    val pre = new PrecisionModel()

    def isContained(quad: XElement): Boolean = {
      if (buffer.getEnvelopeInternal.intersects(quad)) {
        val cps = Array(new Coordinate(quad.xmin, quad.ymin), new Coordinate(quad.xmin, quad.ymin),
          new Coordinate(quad.xmin, quad.ymax),
          new Coordinate(quad.xmax, quad.ymax),
          new Coordinate(quad.xmax, quad.ymin), new Coordinate(quad.xmin, quad.ymin))
        val line = new LinearRing(cps, pre, 4326)
        val polygon = new Polygon(line, pre, 4326)
        polygon.intersects(buffer)
      }
      false
    }


    def checkValue(quad: XElement, level: Short): Unit = {
      if (isContained(quad)) {
        // whole range matches, happy day

        if (level < g) {
          quad.children.foreach(remaining.add)
        }
      }
    }
    //    val startEnv = searTraj.getStartPoint.getEnvelopeInternal
    //    val endEnv = searTraj.getEndPoint.getEnvelopeInternal
    //    searTraj.buffer(threshold)
    //    searTraj.getEnvelopeInternal.expandBy(threshold)
    searTraj.getStartPoint
    //    searTraj.getEndPoint
  }

  private def sequenceInterval(x: Double, y: Double, length: Short, psc: Long, partial: Boolean): (Long, Long) = {
    val min = sequenceCode(x, y, length, psc)
    // if a partial match, we just use the single sequence code as an interval
    // if a full match, we have to match all sequence codes starting with the single sequence code
    val max = if (partial) {
      min
    } else {
      // from lemma 3 in the XZ-Ordering paper
      //min - psc + 3L + (5 * math.pow(4, g - length).toLong - 1)
      min - psc + (5 * math.pow(4, g - length).toLong - 1)
    }
    (min, max)
  }
}

object XZStarSFC {

  // the initial level of quads
  private val LevelOneElements = XElement(-180.0, -90.0, 180.0, 90.0, 360.0).children

  // indicator that we have searched a full level of the quad/oct tree
  private val LevelTerminator = XElement(-1.0, -1.0, -1.0, -1.0, 0)

  private val cache = new java.util.concurrent.ConcurrentHashMap[Short, XZStarSFC]()

  def apply(g: Short): XZStarSFC = {
    var sfc = cache.get(g)
    if (sfc == null) {
      sfc = new XZStarSFC(g, (-180.0, 180.0), (-90.0, 90.0))
      cache.put(g, sfc)
    }
    sfc
  }

  /**
   * Region being queried. Bounds are normalized to [0-1].
   *
   * @param xmin x lower bound in [0-1]
   * @param ymin y lower bound in [0-1]
   * @param xmax x upper bound in [0-1], must be >= xmin
   * @param ymax y upper bound in [0-1], must be >= ymin
   */
  private case class QueryWindow(xmin: Double, ymin: Double, xmax: Double, ymax: Double)

  /**
   * An extended Z curve element. Bounds refer to the non-extended z element for simplicity of calculation.
   *
   * An extended Z element refers to a normal Z curve element that has it's upper bounds expanded by double it's
   * width/height. By convention, an element is always square.
   *
   * @param xmin   x lower bound in [0-1]
   * @param ymin   y lower bound in [0-1]
   * @param xmax   x upper bound in [0-1], must be >= xmin
   * @param ymax   y upper bound in [0-1], must be >= ymin
   * @param length length of the non-extended side (note: by convention width should be equal to height)
   */
  private case class XElement(xmin: Double, ymin: Double, xmax: Double, ymax: Double, length: Double) extends Envelope(xmin, xmax, ymin, ymax) {

    // extended x and y bounds
    lazy val xext = xmax + length
    lazy val yext = ymax + length

    def isContained(window: QueryWindow): Boolean =
      window.xmin <= xmin && window.ymin <= ymin && window.xmax >= xext && window.ymax >= yext

    def overlaps(window: QueryWindow): Boolean =
      window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xext && window.ymin <= yext

    def children: Seq[XElement] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val len = length / 2.0
      val c0 = copy(xmax = xCenter, ymax = yCenter, length = len)
      val c1 = copy(xmin = xCenter, ymax = yCenter, length = len)
      val c2 = copy(xmax = xCenter, ymin = yCenter, length = len)
      val c3 = copy(xmin = xCenter, ymin = yCenter, length = len)
      Seq(c0, c1, c2, c3)
    }
  }

}