package com.just.ksim.experiments.query

import com.just.ksim.disks.Client

import scala.collection.JavaConverters._

object QueryTest {
  def main(args: Array[String]): Unit = {
    val tableName = "tdrive3"
    val client = new Client(tableName)
    val trajectories = client.limit(50)

    for (elem <- trajectories.asScala) {
//      if (elem.getId.equals("3015_1202107149")) {
//        val time = System.currentTimeMillis()
//        val r = client.simQuery(elem, 0.1)
//        println(s"s-${elem.getId},${r.size()},${System.currentTimeMillis() - time}")
//        for (elem <- r.asScala) {
//          println(elem.getId)
//        }
//      }
      val time = System.currentTimeMillis()
      val r = client.simQuery(elem, 0.01)
      println(s"${elem.getId}-s,${r.size()},${System.currentTimeMillis() - time}")
    }

    for (elem <- trajectories.asScala) {
//      if (elem.getId.equals("3015_1202107149")) {
//        val time = System.currentTimeMillis()
//        val result = client.knnQuery2(elem, 50)
//        println(s"knn-${elem.getId},${result.size()},${elem.getNumGeometries},${System.currentTimeMillis() - time},${result.pollLast()._2}")
//      }
      val time = System.currentTimeMillis()
      val result = client.knnQuery2(elem, 50)
      println(s"${elem.getId}-knn,${result.size()},${elem.getNumGeometries},${System.currentTimeMillis() - time},${result.pollLast()._2}")
    }
    client.close()
  }
}
