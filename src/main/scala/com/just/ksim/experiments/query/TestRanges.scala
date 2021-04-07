package com.just.ksim.experiments.query

import com.just.ksim.disks.Client
import com.just.ksim.index.{ElementKNN, XZStarSFC}
import org.locationtech.jts.geom.PrecisionModel

import scala.collection.JavaConverters._

object TestRanges {
  def main(args: Array[String]): Unit = {
    val sfc = XZStarSFC(16, 1)
    val tableName = "tdrive_tmp"
    //val client = new Client(tableName2)
    val client = new Client(tableName)
    val trajectories = client.limit(20)
    for (elem <- trajectories.asScala) {
      //elem.getDPFeature.getIndexes
      val root: ElementKNN = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, 16, new PrecisionModel, 0L)
      val time = System.currentTimeMillis()
      val ranges = sfc.rangesForKnn(elem, 0.3, root)
      val tt = System.currentTimeMillis() - time
      println(s"${elem.getId},${ranges.size()},${elem.getNumGeometries},$tt")
    }
  }
}
