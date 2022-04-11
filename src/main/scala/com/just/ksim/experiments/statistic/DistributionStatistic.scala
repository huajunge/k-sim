package com.just.ksim.experiments.statistic

import com.just.ksim.experiments.query.SimilarityQuery.getTrajectory
import com.just.ksim.index.XZStarSFC
import com.just.ksim.preprocess.HeuristicFilterAndSegment
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.PrecisionModel

import java.util
import scala.collection.JavaConverters._

object DistributionStatistic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("Statistic")
    val sc = new SparkContext(conf)
    val trajPath = args(0)
    val outPath = args(1)
    val maxR = args(2).toShort
    //hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "test_table")
    //IMPORTANT: must set the attribute to solve the problem (can't create path from null string )
    //hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp1")
    val segment = new HeuristicFilterAndSegment(20, 11 * 60)
    val sfc = XZStarSFC.apply(maxR, 1)
    try {
      val pre = new PrecisionModel()
      // val rawRDD = context.wholeTextFiles("D:\\工作文档\\data\\T-drive\\release\\tmp")
      val ps = new util.ArrayList[(String, Long)](maxR)
      val resolutions = new util.ArrayList[(String, Long)](maxR)
      val tmp = sc.textFile(trajPath)
        .map(getTrajectory)
        .filter(t => null != t && t.getNumGeometries >= 6)
        .map(t => {
          try {
            (sfc.index2(t.getMultiPoint)._2, sfc.index2(t.getMultiPoint)._3)
          } catch {
            case _: Exception => null
            case _ => null
          }
        }).filter(t => null != t)
      for (elem <-tmp.map(v => v._1).countByValue()) {
        ps.add((elem._1.toString, elem._2))
      }
      for (elem <-tmp.map(v => v._2).countByValue()) {
        resolutions.add((elem._1.toString, elem._2))
      }
/*      for (elem <- sc
        .textFile(trajPath)
        .map(getTrajectory)
        .filter(t => null != t && t.getNumGeometries >= 4)
        .map(t => {
          try {
            sfc.index2(t.getMultiPoint)._3
          } catch {
            case _: Exception => null
            case _ => null
          }
        }).filter(t => null != t)
        .countByValue()) {
        count.add((elem._1.toString, elem._2))
        println(s"${elem._1},${elem._2}")
      }*/
      val path = new Path(outPath)
      val fs = path.getFileSystem(new Configuration())
      val numbers = ps.asScala.map(v => v._2).sum
      if (!fs.exists(path)) {
        val outputStream = fs.create(path)
        outputStream.writeBytes("position codes: \n")
        for (elem <- ps.asScala) {
          outputStream.writeBytes(s"${elem._1}\t${elem._2}\t${elem._2.toDouble/numbers.toDouble}\n")
        }
        outputStream.writeBytes("resolutions: \n")
        for (elem <- resolutions.asScala) {
          outputStream.writeBytes(s"${elem._1}\t${elem._2}\t${elem._2.toDouble/numbers.toDouble}\n")
        }
        outputStream.flush()
        outputStream.flush()
        outputStream.close()
      } else {
        fs.delete(path)
        val outputStream = fs.create(path)
        outputStream.writeBytes("position codes: \n")
        for (elem <- ps.asScala) {
          outputStream.writeBytes(s"${elem._1}\t${elem._2}\t${elem._2.toDouble/numbers.toDouble}\n")
        }
        outputStream.writeBytes("resolutions: \n")
        for (elem <- resolutions.asScala) {
          outputStream.writeBytes(s"${elem._1}\t${elem._2}\t${elem._2.toDouble/numbers.toDouble}\n")
        }
        outputStream.flush()
        outputStream.flush()
        outputStream.close()
      }
      fs.close()
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
      sc.stop()
    }
  }
}
