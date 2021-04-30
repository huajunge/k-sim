package com.just.ksim.experiments.query

import com.just.ksim.disks.Client
import com.just.ksim.entity.Trajectory
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.MultiPoint
import utils.WKTUtils

import java.util


object TestSize {
  private def getTrajectory(tra: String): Trajectory = {
    val t = tra.split("-")
    new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint])
  }

  def main(args: Array[String]): Unit = {
    val queryTrajFilePath = args(0)
    val trajPath = args(1)
    val k = args(2).toInt
    val outPath = args(3)
    val shard = args(4).toShort
    val interval = args(5).toDouble
    val g = args(6).toShort
    val client = new Client(g, trajPath, shard)
    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("SimilarityQuery")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    var local = false
    try {
      local = args(7).toBoolean
      if (local) {
        conf.setMaster("local[*]")
      }
    } catch {
      case i: Exception =>
    }
    var func = 0
    try {
      func = args(8).toInt
      if (local) {
        conf.setMaster("local[*]")
      }
    } catch {
      case i: Exception =>
    }
    val sc = new SparkContext(conf)
    val timeStatistic = new util.ArrayList[Long](50)
    val queryTrajs = sc
      .textFile(queryTrajFilePath)
      .map(getTrajectory)
      .collect()
    sc.stop()
    Thread.sleep(2000)
    for (elem <- queryTrajs) {
      elem.getDPFeature
      elem.getDPFeature.getIndexes
      elem.getDPFeature.getMBRs
      val time = System.currentTimeMillis()
      val tmp = System.currentTimeMillis() - time
      timeStatistic.add(System.currentTimeMillis() - time)
      val env = elem.getDPFeature.getMBRs().getEnvelopeInternal
      println(s"${elem.getId}-s,$tmp,${elem.getNumGeometries},${elem.getDPFeature.getIndexes.size()},${env.getHeight},${env.getWidth}")
    }

    sc.stop()

    println(s"Query trajectory count: ${queryTrajs.length}")
  }
}
