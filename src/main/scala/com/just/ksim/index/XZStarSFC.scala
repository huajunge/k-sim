package com.just.ksim.index

import com.just.ksim.entity.Trajectory
import org.locationtech.jts.geom._
import org.locationtech.sfcurve.IndexRange

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class XZStarSFC(g: Short, xBounds: (Double, Double), yBounds: (Double, Double), beta: Int) extends Serializable {
  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2

  private val xSize = xHi - xLo
  private val ySize = yHi - yLo

  def index(geometry: Geometry, lenient: Boolean = false): Long = {
    index2(geometry, lenient)._1
  }

  def indexLength(geometry: Geometry, lenient: Boolean = false): String = {
    var s = ""
    for (_ <- 0 to index2(geometry, lenient)._3) {
      s = s + "1"
    }
    s
  }

  def index2(geometry: Geometry, lenient: Boolean = false): (Long, Long, Int) = {
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

  case class Element2(xmin: Double, ymin: Double, xmax: Double, ymax: Double, level: Int, code: Long) {

    def overlaps(traj: Geometry): Boolean = {
      val cps = Array(new Coordinate(xmin, ymin),
        new Coordinate(xmin, ymax),
        new Coordinate(xmax, ymax),
        new Coordinate(xmax, ymin), new Coordinate(xmin, ymin))
      val line = new LinearRing(cps, pre, 4326)
      val polygon = new Polygon(line, null, pre, 4326)
      for (i <- 0 until traj.getNumGeometries) {
        if (polygon.intersects(traj.getGeometryN(i))) {
          return true
        }
      }
      false
    }

    def children: Seq[Element2] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val c0 = copy(xmax = xCenter, ymax = yCenter, level = level + 1, code = code)
      val c1 = copy(xmin = xCenter, ymax = yCenter, level = level + 1, code = code + 1L * Math.pow(4, beta - level).toLong)
      val c2 = copy(xmax = xCenter, ymin = yCenter, level = level + 1, code = code + 2L * Math.pow(4, beta - level).toLong)
      val c3 = copy(xmin = xCenter, ymin = yCenter, level = level + 1, code = code + 3L * Math.pow(4, beta - level).toLong)
      Seq(c0, c1, c2, c3)
    }
  }

  //val ps = Array(0, 0, 0, 1, 0, 2, 0, 3, 0, 0, 0, 5, 0, 6, 7, 4)
  val psMaximum = Array(0, 10, 0, 1, 0, 2, 9, 3, 0, 8, 0, 5, 0, 6, 7, 4)
  val positionIndex = Array(3, 5, 7, 15, 11, 13, 14, 9, 6, 1)

  def signature(x1: Double, y1: Double, x2: Double, y2: Double, traj: Geometry): Long = {
    val remaining = new java.util.ArrayDeque[Element2](Math.pow(4, beta).toInt)
    val levelOneElements = Element2(x1, y1, x2, y2, 1, 0L).children
    val levelTerminator = Element2(-1.0, -1.0, -1.0, -1.0, 0, 0L)
    levelOneElements.foreach(remaining.add)
    remaining.add(levelTerminator)
    var level = 1
    var sig = 0
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next.eq(levelTerminator)) {
        if (!remaining.isEmpty && level < beta) {
          level = (level + 1).toShort
          remaining.add(levelTerminator)
        }
      } else {
        if (next.overlaps(traj)) {
          if (level < beta) {
            next.children.foreach(remaining.add)
          } else {
            sig |= 1 << next.code
          }
        }
      }
    }
    psMaximum(sig).toLong
    //sig
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

  val pre = new PrecisionModel()


  def indexSpace2(env: Envelope, posCode: Long):
  IndexPar = {
    val mbr = env
    val (nxmin, nymin, nxmax, nymax) = normalize(mbr.getMinX, mbr.getMinY, mbr.getMaxX, mbr.getMaxY, false)
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
    val xWidth = w * xSize
    val yWidth = w * ySize
    val sc = sequenceCode(nxmin, nymin, length, posCode)
    val xTrue = x * xSize + xLo
    val yTrue = y * ySize + yLo
    val xMaxTrue = (x + w) * xSize + xLo
    val yMaxTrue = (y + w) * ySize + yLo
    val xCen = (xTrue + xMaxTrue) / 2.0
    val yCen = (yTrue + yMaxTrue) / 2.0
    IndexPar(sc, length, xWidth, yWidth, xTrue, yTrue, xMaxTrue, yMaxTrue, xCen, yCen)
    //(sc, length, xWidth, yWidth, xTrue, yTrue, xMaxTrue, yMaxTrue, xCen, yCen)
  }

  case class IndexPar(var sc: Long, var length: Int, var xWidth: Double, var yWidth: Double, var xTrue: Double, var yTrue: Double, var xMaxTrue: Double, var yMaxTrue: Double, var xCen: Double, var yCen: Double) {
  }

  def rangesForKnn(searTraj: Trajectory, dis: Double, root: ElementKNN): java.util.List[IndexRange] = {
    val ranges = new java.util.ArrayList[IndexRange](100)
    val boundary1 = searTraj.getMultiPoint.getEnvelopeInternal
    val boundaryEnv = searTraj.getMultiPoint.getEnvelopeInternal

    boundary1.expandBy(dis)
    //val buffer = searTraj.buffer(threshold)
    val remaining = new java.util.ArrayDeque[ElementKNN](200)
    val minimumResolution = indexSpace2(boundary1, 0L)

    val levelStop = new ElementKNN(-1, -1, -1, -1, -1, -1, pre, 0)

    remaining.add(root.search(root, minimumResolution.xTrue, minimumResolution.yTrue, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue + minimumResolution.xWidth, minimumResolution.yTrue, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue, minimumResolution.yTrue + minimumResolution.yWidth, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue + minimumResolution.xWidth, minimumResolution.yTrue + minimumResolution.yWidth, minimumResolution.length))

    remaining.add(levelStop)
    var maximumResolution = minimumResolution.length
    var currXS = minimumResolution.xWidth * 2
    var currYS = minimumResolution.yWidth * 2
    //优化
    while ((boundaryEnv.getWidth - currXS) / 2.0 < dis && (boundaryEnv.getHeight - currYS) / 2.0 < dis && maximumResolution < g) {
      maximumResolution += 1
      currXS /= 2.0
      currYS /= 2.0
    }
    //maximumResolution = 16
    //println(s"${minimumResolution.length},$maximumResolution")
    val spoint = searTraj.getGeometryN(0)
    val epoint = searTraj.getGeometryN(searTraj.getNumGeometries - 1)
    var level = minimumResolution.length
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next == levelStop && !remaining.isEmpty && level < maximumResolution) {
        remaining.add(levelStop)
        level = level + 1
      } else {
        if (next.neededToCheck(boundaryEnv, dis)) {
          val candidates = next.checkPositionCode(searTraj, dis, spoint, epoint)
          if (null != candidates) {
            ranges.addAll(candidates)
          }
          if (level < maximumResolution) {
            //next.getChildren.asScala
            next.getChildren.asScala.foreach(v => {
              remaining.add(v)
            })
          }
        }
      }
    }
    if (ranges.size() > 0) {
      ranges.sort(IndexRange.IndexRangeIsOrdered)
      var current = ranges.get(0) // note: should always be at least one range
      val result = ArrayBuffer.empty[IndexRange]
      var i = 1
      while (i < ranges.size()) {
        val range = ranges.get(i)
        if (range.lower <= current.upper + 1) {
          current = IndexRange(current.lower, math.max(current.upper, range.upper), current.contained && range.contained)
        } else {
          result.append(current)
          current = range
        }
        i += 1
      }
      result.append(current)
      result.asJava
    } else {
      ranges
    }
  }

  def xz2RangesForKnn(searTraj: Trajectory, dis: Double, root: ElementKNN): java.util.List[IndexRange] = {
    val ranges = new java.util.ArrayList[IndexRange](100)
    val boundary1 = searTraj.getMultiPoint.getEnvelopeInternal
    val boundaryEnv = searTraj.getMultiPoint.getEnvelopeInternal

    boundary1.expandBy(dis)
    //val buffer = searTraj.buffer(threshold)
    val remaining = new java.util.ArrayDeque[ElementKNN](200)
    val minimumResolution = indexSpace2(boundary1, 0L)

    val levelStop = new ElementKNN(-1, -1, -1, -1, -1, -1, pre, 0)

    remaining.add(root.search(root, minimumResolution.xTrue, minimumResolution.yTrue, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue + minimumResolution.xWidth, minimumResolution.yTrue, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue, minimumResolution.yTrue + minimumResolution.yWidth, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue + minimumResolution.xWidth, minimumResolution.yTrue + minimumResolution.yWidth, minimumResolution.length))

    remaining.add(levelStop)
    var maximumResolution = minimumResolution.length
    var currXS = minimumResolution.xWidth * 2
    var currYS = minimumResolution.yWidth * 2
    //优化
    while ((boundaryEnv.getWidth - currXS) / 2.0 < dis && (boundaryEnv.getHeight - currYS) / 2.0 < dis && maximumResolution < g) {
      maximumResolution += 1
      currXS /= 2.0
      currYS /= 2.0
    }
    //maximumResolution = 16
    //println(s"${minimumResolution.length},$maximumResolution")
    val spoint = searTraj.getGeometryN(0)
    val epoint = searTraj.getGeometryN(searTraj.getNumGeometries - 1)
    var level = minimumResolution.length
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next == levelStop && !remaining.isEmpty && level < maximumResolution) {
        remaining.add(levelStop)
        level = level + 1
      } else {
        if (next.neededToCheck(boundaryEnv, dis)) {
          val candidates = next.xz2CheckPositionCode(searTraj, dis, spoint, epoint)
          if (null != candidates) {
            ranges.addAll(candidates)
          }
          if (level < maximumResolution) {
            //next.getChildren.asScala
            next.getChildren.asScala.foreach(v => {
              remaining.add(v)
            })
          }
        }
      }
    }
    if (ranges.size() > 0) {
      ranges.sort(IndexRange.IndexRangeIsOrdered)
      var current = ranges.get(0) // note: should always be at least one range
      val result = ArrayBuffer.empty[IndexRange]
      var i = 1
      while (i < ranges.size()) {
        val range = ranges.get(i)
        if (range.lower <= current.upper + 1) {
          current = IndexRange(current.lower, math.max(current.upper, range.upper), current.contained && range.contained)
        } else {
          result.append(current)
          current = range
        }
        i += 1
      }
      result.append(current)
      result.asJava
    } else {
      ranges
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