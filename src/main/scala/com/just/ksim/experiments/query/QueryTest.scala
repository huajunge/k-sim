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
    val tableName = "tdrive_tmp"
    val tableName2 = "tdrive_all_0"
    val client = new Client(tableName2)
    val client2 = new Client(tableName)
    val trajectories = client2.limit(50)
    val sfc = XZStarSFC.apply(16.toShort, 1)
    val threshold = 0.02
//    trajectories.asScala.foreach(t => {
//      println(t.getId)
//    })
    val timeStatistic = new util.ArrayList[Long](50)
    for (elem <- trajectories.asScala) {
      //      if (elem.getId.equals("15_1202017525")) {
      //        val time = System.currentTimeMillis()
      //        val r = client.simQuery2(elem, threshold)
      //        println(s"s-${elem.getId},${r.size()},${System.currentTimeMillis() - time}")
      //        for (elem2 <- r.asScala) {
      //          val d = Frechet.calulateDistance(elem.getMultiPoint, elem2.getMultiPoint)
      //          val index = sfc.index2(elem2.getMultiPoint)
      //          val dis = check(elem2.getMultiPoint.getEnvelopeInternal, elem.getMultiPoint.getEnvelopeInternal, threshold)
      //          println(s"${index._1},${index._2},${index._3},$d,${dis}")
      //          if(!dis && d < 0.1) {
      //            println()
      //            Frechet.calulateDistance(elem.getMultiPoint, elem2.getMultiPoint)
      //          }
      //          //          //println(elem2._2)
      //        }
      //        //        for (elem <- r.asScala) {
      //        //          println(elem.getId)
      //        //        }
      //      }
      val time = System.currentTimeMillis()
      val r = client.simQuery(elem, threshold)
      val timeTmp = System.currentTimeMillis() - time
      timeStatistic.add(timeTmp)
      println(s"${elem.getId}-s,${r.size()},${timeTmp}")
    }
    //timeStatistic.sort)
    //ss.asScala.sorted
    var tt = timeStatistic.asScala.sorted
    var sum = tt.sum
    println(s"${tt.max},${tt.min},${sum / tt.size},${tt(24)}")
    timeStatistic.clear()
    //val f = new Frechet()
//    for (elem <- trajectories.asScala) {
//      //          if (elem.getId.equals("15_1202017525")) {
//      //            val time = System.currentTimeMillis()
//      //            val result = client.knnQuery2(elem, 50)
//      //            for (elem2 <- result.asScala) {
//      //              //println(Frechet.calulateDistance(elem.getMultiPoint,elem2._1.getMultiPoint))
//      //              //println(elem2._2)
//      //            }
//      //            println(s"knn-${elem.getId},${result.size()},${elem.getNumGeometries},${System.currentTimeMillis() - time},${result.peekLast()._2}")
//      //          }
//      val time = System.currentTimeMillis()
//      val result = client.knnQuery2(elem, 250)
//      val tmpT = System.currentTimeMillis() - time
//      timeStatistic.add(tmpT)
//      println(s"${elem.getId}-knn,${result.size()},${elem.getNumGeometries},${tmpT},${result.pollLast()._2}")
//    }
//    tt = timeStatistic.asScala.sorted
//    sum = tt.sum
//    println(s"${tt.max},${tt.min},${sum / tt.size},${tt(24)}")
    client.close()
  }
}
