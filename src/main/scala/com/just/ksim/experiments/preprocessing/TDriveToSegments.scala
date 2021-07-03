package com.just.ksim.experiments.preprocessing

import com.just.ksim.entity.Trajectory
import com.just.ksim.preprocess.HeuristicFilterAndSegment
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Envelope, MultiPoint, Point, PrecisionModel}

import java.sql.Timestamp
import java.util
import scala.collection.JavaConverters._

object TDriveToSegments {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TDriveToSegments")
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

    //20, 11*60
    val segment = new HeuristicFilterAndSegment(maxSpeedMeterPerSecond, maxTimeInterval)
    try {
      val pre = new PrecisionModel()
      // val rawRDD = context.wholeTextFiles("D:\\工作文档\\data\\T-drive\\release\\tmp")
      val rawRDD = context.wholeTextFiles(filePath, 100)
      val outPath = outFilePath
      val mbr = new Envelope(-180.0, 180, -90, 90)

      val trajRDD = rawRDD.flatMap(v => {
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
      }).filter(t => null != t && mbr.contains(t.getMultiPoint.getEnvelopeInternal) && t.getNumGeometries >= minSize && t.getNumGeometries <= maxSize).zipWithIndex()
      if (isQuery) {
        context.makeRDD(
          trajRDD.take(querySize).filter(t => t._2 % 5 == 0).flatMap(v => {
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
          })
        ).saveAsTextFile(outPath)
      } else {
        trajRDD.flatMap(v => {
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
        ).saveAsTextFile(outPath)
      }
      //trajRDD.map(t => t.getId).distinct()
    }

    finally {
      context.stop()
    }
  }
}
