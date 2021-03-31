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
import util.Constants.DEFAULT_CF
import util.{PutUtils, WKTUtils}

object StoringTDriveToHBase {
  def main(args: Array[String]): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    val trajPath = args(0)
    val tableName = args(1)
    val countPath = args(2)
    val shards = args(3).toShort
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

    val putUtils = new PutUtils(16.toShort)
    //val shard = 4.toShort
    val time = System.currentTimeMillis()
    val tmp = context.textFile(trajPath, 20).map(tra => {
      val t = tra.split("-")
      putUtils.getPut(new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint]), shards)
    })
    val count = tmp.count()
    val indexingTime = System.currentTimeMillis() - time
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
    tmp.map(put => (new ImmutableBytesWritable(), put)
    ).saveAsHadoopDataset(job)
  }
}
