package com.just.ksim.experiments.preprocessing

import com.just.ksim.entity.Trajectory
import com.just.ksim.preprocess.HeuristicFilterAndSegment
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Envelope, MultiPoint, Point, PrecisionModel}

import java.sql.Timestamp
import java.util
import scala.collection.JavaConverters._

object LorryToSegments {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("LorryToSegments")
    val context = new SparkContext(conf)
    //hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "test_table")
    //IMPORTANT: must set the attribute to solve the problem (can't create path from null string )
    //hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp1")

    val filePath = args(0)
    val outFilePath = args(1)
    //val filePath = "D:\\工作文档\\data\\T-drive\\release\\taxi_log_2008_by_id"
    //val outFilePath = "D:\\工作文档\\data\\T-drive\\release\\segment_0"
    val maxSpeedMeterPerSecond = args(2).toDouble
    val maxTimeInterval = args(3).toInt
    val minSize = args(4).toInt
    val maxSize = args(5).toInt
    val isQuery = args(6).toBoolean
    val querySize = args(7).toInt
    val mbr = new Envelope(-180.0, 180, -90, 90)

    //20, 11*60
    val segment = new HeuristicFilterAndSegment(maxSpeedMeterPerSecond, maxTimeInterval)
    try {
      val pre = new PrecisionModel()
      // val rawRDD = context.wholeTextFiles("D:\\工作文档\\data\\T-drive\\release\\tmp")
      val rawRDD = context.textFile(filePath, 100)
        .map(v => {
          val gps = v.split(",")
          val point = try {
            val day = gps(7).split(" ")(0).split("-")
            val time = gps(7).split(" ")(1).split(":")
            val t = new Timestamp(day(0).toInt + 100, day(1).toInt - 1, day(2).toInt, time(0).toInt, time(1).toInt, time(2).toInt, 0)
            val coordinate = new Coordinate(gps(1).toDouble / Math.pow(10, 6), gps(2).toDouble / Math.pow(10, 6), t.getTime)
            new Point(coordinate, pre, 4326)
          } catch {
            case _: Exception => null
            case _ => null
          }
          (gps(0), point)
        }).filter(v => null != v._2)
        .groupByKey().flatMap(tra => {
        val t = new Trajectory(tra._1, new MultiPoint(tra._2.toArray, pre, 4326))
        segment.filter(t).asScala
      }).filter(t => null != t && mbr.contains(t.getMultiPoint.getEnvelopeInternal) && t.getNumGeometries >= minSize && t.getNumGeometries <= maxSize).zipWithIndex()
      if (isQuery) {
        context.makeRDD(rawRDD.take(querySize).filter(t => t._2  % 5 == 0).flatMap(v => {
          //v._1.setId(v._2.toString)
          val tId = v._2
          var sId = 0
          val segments = new util.ArrayList[String]()
          for (i <- 0 until v._1.getNumGeometries - 1) {
            val s = s"$tId,${v._1.getGeometryN(i).getCoordinate.x},${v._1.getGeometryN(i).getCoordinate.y},${v._1.getGeometryN(i + 1).getCoordinate.x},${v._1.getGeometryN(i + 1).getCoordinate.y},$sId"
            segments.add(s)
            sId += 1
          }
          segments.asScala
        }
        )).saveAsTextFile(outFilePath)
      } else {
        rawRDD.flatMap(v => {
          //v._1.setId(v._2.toString)
          val tId = v._2
          var sId = 0
          val segments = new util.ArrayList[String]()
          for (i <- 0 until v._1.getNumGeometries - 1) {
            val s = s"$tId,${v._1.getGeometryN(i).getCoordinate.x},${v._1.getGeometryN(i).getCoordinate.y},${v._1.getGeometryN(i + 1).getCoordinate.x},${v._1.getGeometryN(i + 1).getCoordinate.y},$sId"
            segments.add(s)
            sId += 1
          }
          segments.asScala
        }
        ).saveAsTextFile(outFilePath)
      }


      context.stop()
    }
  }
}
