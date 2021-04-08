package com.just.ksim.experiments.statistic

import com.just.ksim.entity.Trajectory
import com.just.ksim.index.XZStarSFC
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Envelope, MultiPoint}
import utils.WKTUtils

object Selectivity {
  def main(args: Array[String]): Unit = {
    val trajPath = "D:\\工作文档\\data\\T-drive\\release\\segment_0"
    val conf = new SparkConf().setMaster("local[*]").setAppName("StoringTDrive")
    val context = new SparkContext(conf)
    val sfc = XZStarSFC(16, 1)
    val mbr = new Envelope(-180.0, 180, -90, 90)
    val minSize = 4
    val traDF = context.textFile(trajPath, 10).map(tra => {
      val t = tra.split("-")
      val traj = new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint])
      if (mbr.contains(traj.getMultiPoint.getEnvelopeInternal) && traj.getMultiPoint.getNumGeometries >= minSize) {
        traj
      } else {
        null
      }
    }).filter(v => null != v)
    traDF.count()
    val start = System.currentTimeMillis()
    val index = traDF.map(t => {
      sfc.index(t.getMultiPoint)
    })
    val cnt = index.count()
    println(s"indexing time:${System.currentTimeMillis() - start},$cnt")
    val allIndex = index.distinct().count()
    //    context.textFile(trajPath, 10).map(tra => {
    //      val t = tra.split("-")
    //      val put = putUtils.getPut(new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint]), shard)
    //      (new ImmutableBytesWritable(), put)
    //    }).saveAsHadoopDataset(job)
    println(s"time:${System.currentTimeMillis() - start},${allIndex.toDouble / cnt.toDouble}")
  }
}
