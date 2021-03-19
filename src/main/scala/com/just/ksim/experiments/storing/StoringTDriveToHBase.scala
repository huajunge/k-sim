package com.just.ksim.experiments.storing

import com.just.ksim.entity.Trajectory
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
    val tableName = "tdrive_all_0"
    val trajPath = "D:\\工作文档\\data\\T-drive\\release\\segment_0"
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
    val conf = new SparkConf().setMaster("local[*]").setAppName("StoringTDrive")
    val context = new SparkContext(conf)
    val job = new JobConf(hbaseConf)
    job.setOutputFormat(classOf[TableOutputFormat])
    job.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val putUtils = new PutUtils(16.toShort)
    val shard = 4.toShort
    context.textFile(trajPath,10).map(tra => {
      val t = tra.split("-")
      val put = putUtils.getPut(new Trajectory(t(0), WKTUtils.read(t(1)).asInstanceOf[MultiPoint]), shard)
      (new ImmutableBytesWritable(), put)
    }).saveAsHadoopDataset(job)
  }
}
