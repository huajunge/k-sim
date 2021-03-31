package com.just.ksim.experiments.query

import com.just.ksim.disks.Client
import com.just.ksim.entity.Trajectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.MultiPoint
import util.WKTUtils

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
    val client = new Client(16, trajPath, shard)
    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("SimilarityQuery")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val timeStatistic = new util.ArrayList[Long](50)
    val queryTrajs = sc
      .textFile(queryTrajFilePath)
      .map(getTrajectory)
      .collect()
    sc.stop()
    Thread.sleep(1000)
    for (elem <- queryTrajs) {
      val time = System.currentTimeMillis()
      client.knnQuery(elem, k)
      val tmp = System.currentTimeMillis() - time
      timeStatistic.add(System.currentTimeMillis() - time)
      println(s"${elem.getId}-s,$tmp")
    }
    val csvHeader = "dataVolume\ttype\tk\tmax\tmin\taverage\tmedian"
    val csvLine = new StringBuilder
    var tmpResult = timeStatistic.asScala.sorted
    var sum = tmpResult.sum
    csvLine.append(s"$trajPath\tqueryTime\t$k\t${tmpResult.max}\t${tmpResult.min}\t${sum / tmpResult.size}\t${tmpResult(tmpResult.size / 2)}")

    val path = new Path(outPath + "/statistics_knn")
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
      val outputStream = fs.append(path)
      outputStream.writeBytes("\n")
      outputStream.writeBytes(csvLine.toString())
      println(csvHeader)
      println(csvLine.toString())
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    }
    fs.close()
    sc.stop()

    println(s"Query trajectory count: ${queryTrajs.length}")
  }
}
