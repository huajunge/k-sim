package just.urbancomputing.index

import org.locationtech.jts.geom._

import scala.language.higherKinds

class XZSFC(g: Short, xBounds: (Double, Double), yBounds: (Double, Double), beta: Int) {
  val xLo = xBounds._1
  val xHi = xBounds._2
  val yLo = yBounds._1
  val yHi = yBounds._2

  val xSize = xHi - xLo
  val ySize = yHi - yLo
  val psMaximum = Array(0, 10, 0, 1, 0, 2, 9, 3, 0, 8, 0, 5, 0, 6, 7, 4)
  val positionIndex = Array(3, 5, 7, 15, 11, 13, 14, 9, 6, 1)

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
}

object XZSFC {

  val DefaultPrecision: Short = 12

  val LogPointFive: Double = math.log(0.5)

}
