package com.just.ksim.urbancomputing.experiments

import com.just.ksim.urbancomputing.dao.Client
import com.just.ksim.urbancomputing.entity.Trajectory
import com.just.ksim.urbancomputing.utils.WKTUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.MultiPoint

import java.util
import scala.collection.JavaConverters._


object KNNQuery {
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
      .setMaster("local[*]")
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
    val removeSet = List("100", "105", "110", "115", "120", "125", "130", "135", "140", "150", "160", "165", "170", "175", "195", "210", "215", "220", "235", "250", "305", "310", "335", "345", "370", "375", "380")

    for (elem <- queryTrajs) {
      if (!removeSet.contains(elem.getId)) {
        elem.getDPFeature
        elem.getDPFeature.getIndexes
        elem.getDPFeature.getMBRs
        val time = System.currentTimeMillis()
        val result = client.knnQuery(elem, k, func)
        val tmp = System.currentTimeMillis() - time
        timeStatistic.add(System.currentTimeMillis() - time)
//        val ite = result.iterator()
//        while(ite.hasNext) {1
//          val t = ite.next()
//          println(s"${t._1.getId},${t._2}")
//        }
        println(s"${elem.getId}-s,$tmp, ${result.size()},${result.peekLast()._2}")
        Thread.sleep(200)
      }
    }
    val csvHeader = "dataVolume\ttype\tk\tmax\tmin\taverage\tmedian"
    val csvLine = new StringBuilder
    var tmpResult = timeStatistic.asScala.sorted
    var sum = tmpResult.sum
    csvLine.append(s"$trajPath\tqueryTime\t$k\t${tmpResult.max}\t${tmpResult.min}\t${sum / tmpResult.size}\t${tmpResult(tmpResult.size / 2 - 1)}")

    val path = new Path(outPath)
    val fs = path.getFileSystem(new Configuration())
    if (!fs.exists(path)) {
      val outputStream = fs.create(path)
      outputStream.writeBytes(csvHeader)
      outputStream.writeBytes("\n")
      outputStream.writeBytes(csvLine.toString())
      println(csvHeader)
      println(csvLine.toString())
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    } else {
      fs.delete(path)
      val outputStream = fs.create(path)
      outputStream.writeBytes("\n")
      outputStream.writeBytes(csvLine.toString())
      println(csvHeader)
      println(csvLine.toString())
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    }
    fs.close()

    println(s"Query trajectory count: ${queryTrajs.length}")
  }
}
