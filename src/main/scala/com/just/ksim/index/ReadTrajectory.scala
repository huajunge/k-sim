package com.just.ksim.index

import com.just.ksim.disks.Client
import com.just.ksim.entity.Trajectory
import org.locationtech.jts.geom.{Coordinate, MultiPoint, Point, PrecisionModel}

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
    val client = new Client("test-ksim5")
    files.listFiles().foreach(f => {
      val source = Source.fromFile(f)
      val tid = f.getName
      val points = new java.util.ArrayList[Point](100)
      var size = 0
      for (line: String <- source.getLines()) {
        size += 1
        if (size < 100) {
          val gps = line.split(",")
          val t = Timestamp.valueOf(gps(0))
          val coordinate = new Coordinate(gps(3).toDouble, gps(2).toDouble, t.getTime)
          points.add(new Point(coordinate, pre, 4326))
        }
        //        val gps = line.split(",")
        //        val t = Timestamp.valueOf(gps(0))
        //        val coordinate = new Coordinate(gps(3).toDouble, gps(2).toDouble, t.getTime)
        //        points.add(new Point(coordinate, pre, 4326))
      }
      val ps = points.toArray(new Array[Point](points.size))
      //val traj = new Trajectory(tid, ps, pre, 4326)
      val traj = new Trajectory(tid, new MultiPoint(ps, pre, 4326))
      trajs.add(traj)
      trajMap.put((traj.getId, sfc.index(traj.getMultiPoint)), traj)
      //client.insert(traj)
      //println()
      //val sc = sfc.index(traj)
      //println(traj.getId + "," + sc.toString)
    })
    //trajs.get(0)
    val threshold = 0.18

    var st = System.currentTimeMillis()
    val simTrajs = client.simQuery(trajs.get(0), threshold)
    println(s"${System.currentTimeMillis() - st},${simTrajs.size()}")

    st = System.currentTimeMillis()
    val result = client.knnQuery2(trajs.get(0), 5)
    println(s"${System.currentTimeMillis() - st},${result.size()}")
    //    trajMap.entrySet().forEach(new Consumer[java.util.Map.Entry[(String, Long), Trajectory]] {
    //      protected override def accept(t: java.util.Map.Entry[(String, Long), Trajectory]): Unit = {
    //        //val st = System.currentTimeMillis()
    //        //client.query(t.getValue, threshold)
    //        println(s"${t.getValue.distance(trajs.get(0))}")
    //      }
    //    })
    client.close()
  }
}
