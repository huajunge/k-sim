package com.just.ksim.experiments.statistic

import com.just.ksim.entity.Trajectory
import com.just.ksim.index.XZStarSFC
import com.just.ksim.preprocess.HeuristicFilterAndSegment
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, MultiPoint, Point, PrecisionModel}

import java.sql.Timestamp
import scala.collection.JavaConverters._

object Statistic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Statistic")
    val context = new SparkContext(conf)
    //hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "test_table")
    //IMPORTANT: must set the attribute to solve the problem (can't create path from null string )
    //hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp1")
    val segment = new HeuristicFilterAndSegment(20, 11 * 60)
    val sfc = XZStarSFC.apply(16, 1)
    try {
      val pre = new PrecisionModel()
      val rdd = context.makeRDD(1 to 100000)
      // val rawRDD = context.wholeTextFiles("D:\\工作文档\\data\\T-drive\\release\\tmp")
      val rawRDD = context.wholeTextFiles("D:\\工作文档\\data\\T-drive\\release\\taxi_log_2008_by_id",10)
      for (elem <- rawRDD.flatMap(v => {
        val nameFriIndex = v._1.lastIndexOf("/")
        val nameLastIndex = v._1.lastIndexOf(".")
        val name = v._1.substring(nameFriIndex + 1, nameLastIndex)
        //val points = new java.util.ArrayList[Point](100)
        val points = v._2.split("\n").map(value => {
          try {
            val gps = value.split(",")
            val t = Timestamp.valueOf(gps(1))
            val coordinate = new Coordinate(gps(2).toDouble, gps(3).toDouble, t.getTime)
            new Point(coordinate, pre, 4326)
          } catch {
            case _: Exception => null
            case _ => null
          }
        }).filter(p => null != p)
        //val ps = points.toArray(new Array[Point](points.size))
        //val traj = new Trajectory(tid, ps, pre, 4326)
        if (null != points) {
          val tra = new Trajectory(name, new MultiPoint(points, pre, 4326))
          segment.filter(tra).asScala
        } else {
          null
        }
      }).filter(t => null != t && t.getNumGeometries >= 4)
        .map(t => {
          try {
            sfc.index2(t.getMultiPoint)._3
          }catch {
            case _: Exception => null
            case _ => null
          }
        }).filter(t => null != t)
        .countByValue()) {
        println(s"${elem._1},${elem._2}")
      }
      // column family
      //      val family = Bytes.toBytes(DEFAULT_CF)
      //      // column counter --> ctr
      //      val column = Bytes.toBytes("ctr")
      //
      //      rdd.map(value => {
      //        var put = new Put(Bytes.toBytes(value))
      //        put.addImmutable(family, column, Bytes.toBytes(value))
      //        (new ImmutableBytesWritable(), put)
      //      })
      //        .saveAsHadoopDataset(job)
    }

    finally {
      context.stop()
    }
  }
}
