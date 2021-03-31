package com.just.ksim.experiments.statistic

import com.just.ksim.disks.Client
import com.just.ksim.index.XZStarSFC

import java.util
import scala.collection.JavaConverters._

object FinalCount {
  def main(args: Array[String]): Unit = {
    val tableName = "tdrive_tmp"
    val tableName2 = "tdrive_all_0"
    val client = new Client(tableName2)
    val client2 = new Client(tableName)
    val trajectories = client2.limit(200)
    val sfc = XZStarSFC.apply(16.toShort, 1)
    val threshold = 0.005
    //    trajectories.asScala.foreach(t => {
    //      println(t.getId)
    //    })
    val timeStatistic = new util.ArrayList[Long](50)
    var ii = 0
    val interval = 4
    for (elem <- trajectories.asScala) {
      if (ii % interval == 0) {
        val time = System.currentTimeMillis()
        val r = client.simQueryCount(elem, threshold)
        val timeTmp = System.currentTimeMillis() - time
        timeStatistic.add(timeTmp)
        println(s"${elem.getId}-s,${r},${timeTmp}")
        val rr = client.simQuery(elem, threshold)
        println("---------")
        println("---------")
        println(s"${elem.getId}-s,${rr.size()},${timeTmp}")
      }
      ii += 1
    }
  }
}
