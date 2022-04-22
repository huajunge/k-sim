package com.just.ksim.experiments.statistic

import com.just.ksim.entity.Trajectory
import com.just.ksim.index.XZStarSFC
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Envelope, MultiPoint}
import utils.WKTUtils

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter

object Selectivity {
  def main(args: Array[String]): Unit = {
    val trajPath = args(0)
    val outPath = args(1)
    val startResolution = args(2).toLong
    val endResolution = args(3).toLong
    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("Selectivity")
    val context = new SparkContext(conf)
    val mbr = new Envelope(-180.0, 180, -90, 90)
    val minSize = 6

    val traDF = context.textFile(trajPath, 20).map(tra => {
      val t = tra.split("-")
      val traj = new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint])
      if (mbr.contains(traj.getMultiPoint.getEnvelopeInternal) && traj.getMultiPoint.getNumGeometries >= minSize) {
        traj
      } else {
        null
      }
    }).filter(v => null != v)
    traDF.persist()
    val cnt = traDF.count()
    val selectivity = new util.ArrayList[(String, Long, Double)](20)
    for (i <- startResolution to endResolution) {
      val sfc = XZStarSFC(i.toShort, 1)
      val start = System.currentTimeMillis()
      val index = traDF.map(t => {
        sfc.index2(t.getMultiPoint)._1
      })
      //val cnt = index.count()
      val time = System.currentTimeMillis() - start
      println(s"indexing time:${time},$cnt")
      val allIndex = index.distinct().count()
      //    context.textFile(trajPath, 10).map(tra => {
      //      val t = tra.split("-")
      //      val put = putUtils.getPut(new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint]), shard)
      //      (new ImmutableBytesWritable(), put)
      //    }).saveAsHadoopDataset(job)
      println(s"$i, time:${System.currentTimeMillis() - start},${allIndex.toDouble / cnt.toDouble}")
      selectivity.add((i.toString, time, allIndex.toDouble / cnt.toDouble))
      //      index.map(v => v._2).countByValue().foreach(elem => {
      //        println(s"${elem._1},${elem._2.toDouble / cnt.toDouble}")
      //      })
      println("----------")
      println("----------")
      println("----------")
    }
    val path = new Path(outPath)
    val fs = path.getFileSystem(new Configuration())
    //val numbers = ps.asScala.map(v => v._2).sum
    if (!fs.exists(path)) {
      val outputStream = fs.create(path)
      outputStream.writeBytes("Selectivity: \n")
      for (elem <- selectivity.asScala) {
        outputStream.writeBytes(s"${elem._1}\t${elem._2}\t${elem._3}\n")
      }
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    } else {
      fs.delete(path)
      val outputStream = fs.create(path)
      outputStream.writeBytes("Selectivity: \n")
      for (elem <- selectivity.asScala) {
        outputStream.writeBytes(s"${elem._1}\t${elem._2}\t${elem._3}\n")
      }
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    }
    fs.close()
    context.stop()
  }
}
