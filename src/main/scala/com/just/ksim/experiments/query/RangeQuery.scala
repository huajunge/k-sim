package com.just.ksim.experiments.query

import com.just.ksim.disks.Client
import com.just.ksim.entity.Trajectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.MultiPoint
import utils.WKTUtils

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter


object RangeQuery {
  private def getTrajectory(tra: String): Trajectory = {
    val t = tra.split("-")
    new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint])
  }

  def main(args: Array[String]): Unit = {
    val mbrs = args(0).split(";")
    val trajPath = args(1)
    val outPath = args(2)
    val shard = args(3).toShort
    val g = args(4).toShort

    val client = new Client(g, trajPath, shard)
    val conf = new SparkConf()
    client.rangeQuery(mbrs(0).split(",")(0).toDouble,mbrs(0).split(",")(1).toDouble,mbrs(0).split(",")(2).toDouble,mbrs(0).split(",")(3).toDouble)
    client.rangeQuery(mbrs(0).split(",")(0).toDouble,mbrs(0).split(",")(1).toDouble,mbrs(0).split(",")(2).toDouble,mbrs(0).split(",")(3).toDouble)
    Thread.sleep(200)
    val timeStatistic = new util.ArrayList[Long](10)
    val sizeStatistic = new util.ArrayList[Long](10)
    for (mbrbox <- mbrs) {
      val mbr = mbrbox.split(",")
      val time = System.currentTimeMillis()
      val results = client.rangeQuery(mbr(0).toDouble,mbr(1).toDouble,mbr(2).toDouble,mbr(3).toDouble)
      val tmp = System.currentTimeMillis() - time
      timeStatistic.add(tmp)
      sizeStatistic.add(results)
      //println(results.size())
    }
    val sc = new SparkContext(conf)
    var tmpResult = timeStatistic.asScala.sorted
    var sum = tmpResult.sum
    val csvLine = new StringBuilder
    csvLine.append(s"queryTime\t${tmpResult.max}\t${tmpResult.min}\t${sum / tmpResult.size}\t${tmpResult(tmpResult.size / 2 - 1)}")
    val path = new Path(outPath)
    val fs = path.getFileSystem(new Configuration())
    if (!fs.exists(path)) {
      val outputStream = fs.create(path)
      outputStream.writeBytes(csvLine.toString())
      outputStream.writeBytes("\n")
      for (i <- 0 until mbrs.size) {
        outputStream.writeBytes(s"MBR:${mbrs(i)}\t time: ${timeStatistic.get(i)} ")
        outputStream.writeBytes("\n")
      }
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    } else {
      fs.delete(path)
      val outputStream = fs.create(path)
      outputStream.writeBytes(csvLine.toString())
      outputStream.writeBytes("\n")
      for (i <- 0 until mbrs.size) {
        outputStream.writeBytes(s"MBR:${mbrs(i)}\t time: ${timeStatistic.get(i)}")
        outputStream.writeBytes("\n")
      }
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    }
    fs.close()
  }
}
