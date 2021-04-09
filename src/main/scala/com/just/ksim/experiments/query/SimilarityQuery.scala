package com.just.ksim.experiments.query

import com.just.ksim.disks.Client
import com.just.ksim.entity.Trajectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.MultiPoint
import utils.WKTUtils

import java.util
import scala.collection.JavaConverters._


object SimilarityQuery {
  private def getTrajectory(tra: String): Trajectory = {
    val t = tra.split("-")
    new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint])
  }

  def main(args: Array[String]): Unit = {
    val queryTrajFilePath = args(0)
    val trajPath = args(1)
    val threshold = args(2).toDouble
    val outPath = args(3)
    val shard = args(4).toShort

    val client = new Client(16, trajPath, shard)

    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("SimilarityQuery")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    var local = false
    try {
      local = args(5).toBoolean
      if(local) {
        conf.setMaster("local[*]")
      }
    } catch {
      case i: Exception =>
    }
    val sc = new SparkContext(conf)
    val timeStatistic = new util.ArrayList[Long](50)
    val count = new util.ArrayList[Long](50)
    val queryTrajs = sc
      .textFile(queryTrajFilePath)
      .map(getTrajectory)
      .collect()
    Thread.sleep(2000)
    for (elem <- queryTrajs) {
      elem.getDPFeature
      elem.getDPFeature.getIndexes
      elem.getDPFeature.getMBRs
      val time = System.currentTimeMillis()
      client.simQuery(elem, threshold)
      val tmp = System.currentTimeMillis() - time
      timeStatistic.add(tmp)
      val size = client.simQueryCount(elem, threshold)
      count.add(size)
      //println(s"${elem.getId}-s,$size,$tmp")
    }
    val csvHeader = "dataVolume\ttype\tthreshold\tmax\tmin\taverage\tmedian\ttrajNum"
    val csvLine = new StringBuilder
    var tmpResult = timeStatistic.asScala.sorted
    var sum = tmpResult.sum
    csvLine.append(s"$trajPath\tqueryTime\t$threshold\t${tmpResult.max}\t${tmpResult.min}\t${sum / tmpResult.size}\t${tmpResult(tmpResult.size / 2 - 1)}")

    tmpResult = count.asScala.sorted
    sum = tmpResult.sum
    csvLine.append("\n").append(s"$trajPath\tnumber\t$threshold\t${tmpResult.max}\t${tmpResult.min}\t${sum / tmpResult.size}\t${tmpResult(tmpResult.size / 2 - 1)}")

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
    sc.stop()

    println(s"Query trajectory count: ${queryTrajs.length}")
  }
}
