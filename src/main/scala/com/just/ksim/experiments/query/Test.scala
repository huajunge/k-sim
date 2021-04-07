package com.just.ksim.experiments.query

import com.just.ksim.disks.Client
import com.just.ksim.index.{ElementKNN, XZStarSFC}
import org.locationtech.jts.geom.PrecisionModel

import scala.collection.JavaConverters._

object Test {
  def main(args: Array[String]): Unit = {
    val root: ElementKNN = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, 16, new PrecisionModel, 0L)
    val sfc = XZStarSFC(16, 1)
    val tableName = "tdrive_tmp"
    //val client = new Client(tableName2)
    val client = new Client(tableName)
    val trajectories = client.limit(100)
    for (elem <- trajectories.asScala) {
      elem.getId
    }
    for (elem <- trajectories.asScala) {
      val time = System.currentTimeMillis()
      val ps = elem.getDPFeature.getPivot
      val size = elem.getDPFeature.getPivot.size()
      val tt = System.currentTimeMillis() - time
      //val ranges: util.List[IndexRange] = sfc.rangesForKnn(elem, 0.4, root)
      //println(s"${elem.getId},${elem.getNumGeometries},${System.currentTimeMillis() - time}")
      println(s"${tt},${elem.getId},${elem.getNumGeometries},${size},${ps}")
      println(s"${elem.getMultiPoint.toText}")
      for (elem <- ps.asScala) {
        println(elem._1)
      }
      //println(s"${ps.asScala.map(v => v._1)}")
      var sum = 0
      var max = 0.0
      for (i <- 0 until elem.getNumGeometries) {
        var minDis = 4444444.0
        var tag = true
        for (pv <- ps.asScala) {
          if (null != pv._1 && (pv._1.intersects(elem.getGeometryN(i))|| pv._1.contains(elem.getGeometryN(i)))&& tag) {
            sum += 1
            tag = false
          }
          if (null != pv._1) {
            val d = pv._1.distance(elem.getGeometryN(i))
            if (minDis > d) {
              minDis = d
            }
          }
        }
        if (tag) {
          //println(i)
        }
        if (max < minDis) {
          max = minDis
        }
      }
      println(s"$sum,$max")
    }
  }
}
