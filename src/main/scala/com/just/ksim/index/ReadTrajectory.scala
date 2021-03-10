package com.just.ksim.index

import com.just.ksim.disks.Client
import com.just.ksim.entity.Trajectory
import org.locationtech.jts.geom.{Coordinate, Point, PrecisionModel}

import java.io.File
import java.sql.Timestamp
import scala.io.Source

object ReadTrajectory {
  def main(args: Array[String]): Unit = {
    val files = new File("D:\\工作文档\\data\\gy_tra\\tmp")
    val trajs = new java.util.ArrayList[Trajectory](100)
    val trajMap = new java.util.HashMap[(String, Long), Trajectory](100)
    val pre = new PrecisionModel()
    val sfc = XZStarSFC(16, 1)
    val client = new Client("test-ksim")
    files.listFiles().foreach(f => {
      val source = Source.fromFile(f)
      val tid = f.getName
      val points = new java.util.ArrayList[Point](100)
      for (line: String <- source.getLines()) {
        val gps = line.split(",")
        val t = Timestamp.valueOf(gps(0))
        val coordinate = new Coordinate(gps(3).toDouble, gps(2).toDouble, t.getTime)
        points.add(new Point(coordinate, pre, 4326))
      }
      val ps = points.toArray(new Array[Point](points.size))
      val traj = new Trajectory(tid, ps, pre, 4326)
      trajs.add(traj)
      trajMap.put((traj.getId, sfc.index(traj)), traj)
      //client.insert(traj)
      //println()
      //val sc = sfc.index(traj)
      //println(traj.getId + "," + sc.toString)
    })
    //trajs.get(0)
    val threshold = 0.001
    //    trajMap.entrySet().forEach(new Consumer[java.util.Map.Entry[(String, Long), Trajectory]] {
    //      protected override def accept(t: java.util.Map.Entry[(String, Long), Trajectory]): Unit = {
    //        val start = System.currentTimeMillis()
    //        val range = sfc.simRange(t.getValue, threshold)
    //        val time = System.currentTimeMillis() - start
    //        var isOK = false
    //        for (r <- range.toArray()) {
    //          if (r.equals(t.getKey._2)) {
    //            isOK = true
    //          }
    //        }
    //        println(s"${t.getKey._1},${t.getKey._2},$time, ${range.size()}, $isOK")
    //      }
    //    })

    //client.query(trajs.get(0), threshold)
    val st = System.currentTimeMillis()
    client.query(trajs.get(0), threshold)
    println(s"${System.currentTimeMillis() - st}")
//    trajMap.entrySet().forEach(new Consumer[java.util.Map.Entry[(String, Long), Trajectory]] {
//      protected override def accept(t: java.util.Map.Entry[(String, Long), Trajectory]): Unit = {
//        val st = System.currentTimeMillis()
//        //client.query(t.getValue, threshold)
//        println(s"${System.currentTimeMillis() - st}")
//      }
//    })

    client.close()
  }
}
