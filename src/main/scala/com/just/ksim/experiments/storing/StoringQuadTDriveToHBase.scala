package com.just.ksim.experiments.storing

import com.just.ksim.entity.Trajectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.MultiPoint
import utils.Constants.DEFAULT_CF
import utils.{PutUtils, WKTUtils}

object StoringQuadTDriveToHBase {
  def main(args: Array[String]): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    val trajPath = args(0)
    val tableName = args(1)
    val countPath = args(2)
    val shards = args(3).toShort
    val dataVolume = args(4).toInt
    val endVolume = args(5).toInt
    var g = 16.toShort

    try {
      g =args(6).toShort
    } catch {
      case i: Exception =>
    }
    //    val tableName = "tdrive_throughput"
    //    val trajPath = "D:\\工作文档\\data\\T-drive\\release\\segment_0"
    //val trajPath = "D:\\工作文档\\data\\T-drive\\release\\out"
    //val trajPath = "D:\\工作文档\\data\\T-drive\\release\\out\\part-00000"
    val table = new HTableDescriptor(TableName.valueOf(tableName))
    if (!admin.tableExists(table.getTableName)) {
      val table = new HTableDescriptor(TableName.valueOf(tableName))
      if (admin.tableExists(table.getTableName)) {
        admin.disableTable(table.getTableName)
        admin.deleteTable(table.getTableName)
      }
      table.addFamily(new HColumnDescriptor(DEFAULT_CF))
      admin.createTable(table)
    }
    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("StoringTDrive")
    val context = new SparkContext(conf)
    val job = new JobConf(hbaseConf)
    job.setOutputFormat(classOf[TableOutputFormat])
    job.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val putUtils = new PutUtils(g)
    var count = 0L
    var indexingTime = 0L
    //val shard = 4.toShort
    var size = 0L
    var filePath = trajPath + "/" + dataVolume
    for (i <- dataVolume + 1 to endVolume) {
      filePath += "," + trajPath + "/" + i
    }
    val time = System.currentTimeMillis()
    val tmp2 = context.textFile(filePath, 100)
    val tmp = tmp2.zipWithIndex().map(tra => {
      val t = tra._1.split("\t")
      val tid = tra._2
      putUtils.getQuadTreePut(new Trajectory(tid.toString, WKTUtils.read(t(2)).asInstanceOf[MultiPoint]), shards)
    })
    count = tmp.count()
    //size = count
    indexingTime = System.currentTimeMillis() - time
    tmp.map(put => (new ImmutableBytesWritable(), put)).saveAsHadoopDataset(job)

    val path = new Path(countPath)
    val fs = path.getFileSystem(new Configuration())
    if (!fs.exists(path)) {
      val outputStream = fs.create(path)
      outputStream.writeBytes(count.toString)
      outputStream.writeBytes("\t")
      outputStream.writeBytes(indexingTime.toString)
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    } else {
      val outputStream = fs.append(path)
      outputStream.writeBytes(count.toString)
      outputStream.writeBytes("\t")
      outputStream.writeBytes(indexingTime.toString)
      outputStream.flush()
      outputStream.flush()
      outputStream.close()
    }
    fs.close()

  }
}
