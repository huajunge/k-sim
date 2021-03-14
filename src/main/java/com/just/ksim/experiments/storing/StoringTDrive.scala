package com.just.ksim.experiments.storing

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import util.Constants.DEFAULT_CF

object StoringTDrive {
  def main(args: Array[String]): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    val tableName = "test_tdrive"
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
    //hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "test_table")
    //IMPORTANT: must set the attribute to solve the problem (can't create path from null string )
    //hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp1")

    val job = new JobConf(hbaseConf)
    job.setOutputFormat(classOf[TableOutputFormat])
    job.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    //job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    //job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    //job.setOutputValueClass(classOf[Put])

    try {
      val rdd = context.makeRDD(1 to 100000)
      context.wholeTextFiles("D:\\工作文档\\data\\T-drive\\release\\tmp")
      // column family
      val family = Bytes.toBytes(DEFAULT_CF)
      // column counter --> ctr
      val column = Bytes.toBytes("ctr")

      rdd.map(value => {
        var put = new Put(Bytes.toBytes(value))
        put.addImmutable(family, column, Bytes.toBytes(value))
        (new ImmutableBytesWritable(), put)
      })
        .saveAsHadoopDataset(job)
    } finally {
      context.stop()
    }
  }
}
