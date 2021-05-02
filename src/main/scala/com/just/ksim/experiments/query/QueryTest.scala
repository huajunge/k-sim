package com.just.ksim.experiments.query

import com.just.ksim.disks.Client
import com.just.ksim.index.XZStarSFC
import org.locationtech.jts.geom.Envelope

import java.util
import scala.collection.JavaConverters._

object QueryTest {
  def check(env1: Envelope, env2: Envelope, threshold: Double): Boolean = {
    //val enlElement = new Envelope(xmin, xmax + xLength, ymin, ymax + yLength)
    env1.expandBy(threshold)
    env1.contains(env2)
  }

  def main(args: Array[String]): Unit = {
    val tableName = "tdrive_p"
    val tableName2 = "tdrive_p"
    val client = new Client(tableName2)
    val client2 = new Client(tableName)
    val trajectories = client2.limit(200)
    val sfc = XZStarSFC.apply(16.toShort, 1)
    val threshold = 0.02
    //    trajectories.asScala.foreach(t => {
    //      println(t.getId)
    //    })
    val timeStatistic = new util.ArrayList[Long](50)
    var ii = 0
    val interval = 4
    for (elem <- trajectories.asScala) {
      if (ii % interval == 0 && elem.getNumGeometries > 5) {
        val time = System.currentTimeMillis()
        val r = client.simQuery(elem, threshold, 1)
        val timeTmp = System.currentTimeMillis() - time
        timeStatistic.add(timeTmp)
        println(s"${elem.getId}-s,${r.size()},${timeTmp}")
      }
      ii += 1
    }
    //timeStatistic.sort)
    //ss.asScala.sorted
    var tt = timeStatistic.asScala.sorted
    var sum = tt.sum
    println(s"${tt.max},${tt.min},${sum / tt.size},${tt(12)}")
    timeStatistic.clear()
    //val f = new Frechet()
    var i = 0

    for (elem <- trajectories.asScala) {
      if (i % interval == 0) {
        elem.getDPFeature.getIndexes
        elem.getDPFeature.getMBRs
        val time = System.currentTimeMillis()
        val result = client.knnQuery(elem, 50, 0.002, 0)
        val tmpT = System.currentTimeMillis() - time
        timeStatistic.add(tmpT)
        println(s"${elem.getId}-knn,,${elem.getNumGeometries},${tmpT}")
      }
      i += 1
    }
    tt = timeStatistic.asScala.sorted
    sum = tt.sum
    println(s"${tt.max},${tt.min},${sum / tt.size},${tt(12)}")
    client.close()
  }
}
