package com.just.ksim.experiments.storing

import com.just.ksim.index.XZStarSFC
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.MultiPoint
import utils.WKTUtils

object StoringTDriveToHBase {
  def main(args: Array[String]): Unit = {
    val trajPath = args(0)
    val tableName = args(1)
    val countPath = args(2)
    val shards = args(3).toShort
    val dataVolume = args(4).toInt
    val endVolume = args(5).toInt
    var g = 16.toShort
    val sfc = XZStarSFC.apply(g, 1)
    try {
      g = args(6).toShort
    } catch {
      case i: Exception =>
    }
    //    val tableName = "tdrive_throughput"
    //    val trajPath = "D:\\工作文档\\data\\T-drive\\release\\segment_0"
    //val trajPath = "D:\\工作文档\\data\\T-drive\\release\\out"
    //val trajPath = "D:\\工作文档\\data\\T-drive\\release\\out\\part-00000"
    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("StoringTDrive")
    val context = new SparkContext(conf)

    //val shard = 4.toShort
    var filePath = trajPath + "/" + dataVolume
    for (i <- dataVolume + 1 to endVolume) {
      filePath += "," + trajPath + "/" + i
    }
    val time = System.currentTimeMillis()
    val tmp2 = context.textFile(filePath, 100)
    val tmp = tmp2.zipWithIndex().map(tra => {
      val t = tra._1.split("-")
      val tid = tra._2

      val integerKey = tid.toString.length + 9
      val length = Bytes.toBytes(sfc.indexLength(WKTUtils.read(t(1)).asInstanceOf[MultiPoint])).length
      val stringKey = tid.toString.length + length + 1
      //putUtils.getPut(new Trajectory(tid.toString, WKTUtils.read(t(1)).asInstanceOf[MultiPoint]), shards)
      (integerKey.toLong, stringKey.toLong)
    })
    val result = tmp.reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    val path = new Path(countPath)
    val fs = path.getFileSystem(new Configuration())
    if (!fs.exists(path)) {
      val outputStream = fs.create(path)
      outputStream.writeBytes("integer: ")
      outputStream.writeBytes((result._1 / 8).toString)
      outputStream.writeBytes("\n")
      outputStream.writeBytes("string: ")
      outputStream.writeBytes((result._2 / 8).toString)
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    } else {
      val outputStream = fs.append(path)
      outputStream.writeBytes("integer: ")
      outputStream.writeBytes((result._1 / 8).toString)
      outputStream.writeBytes("\n")
      outputStream.writeBytes("string: ")
      outputStream.writeBytes((result._2 / 8).toString)
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    }
    fs.close()

  }
}
