package com.just.ksim.experiments.query

object RtreeQuery {
//  def main(args: Array[String]): Unit = {
//    val queryPath = args(0)
//    val trajPath = args(1)
//    val tableName = args(2)
//    val conf = new SparkConf()
//      //.setMaster("local[*]")
//      .setAppName("SimilarityQuery")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val sc = new SparkContext(conf)
//    val queryMBRs = sc
//      .textFile(queryPath)
//      .collect()
//    var time = System.currentTimeMillis()
//    val hbaseConf = HBaseConfiguration.create()
//    val connection = ConnectionFactory.createConnection(hbaseConf)
//    val admin = connection.getAdmin
//    val job = new JobConf(hbaseConf)
//    job.setOutputFormat(classOf[TableOutputFormat])
//    job.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//    val table = new HTableDescriptor(TableName.valueOf(tableName))
//    if (!admin.tableExists(table.getTableName)) {
//      val table = new HTableDescriptor(TableName.valueOf(tableName))
//      if (admin.tableExists(table.getTableName)) {
//        admin.disableTable(table.getTableName)
//        admin.deleteTable(table.getTableName)
//      }
//      table.addFamily(new HColumnDescriptor(DEFAULT_CF))
//      admin.createTable(table)
//    }
//    val tmp2 = sc.textFile(trajPath, 100).zipWithIndex().map(tra => {
//      val traj = tra._1.split("\t")(1).asInstanceOf[MultiPoint]
//      val tid = tra._2
//      (tid, traj.toString, traj.getEnvelopeInternal)
//    })
//    tmp2.map(t => {
//      val put = new Put(Bytes.toBytes(t._1))
//      put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes("traj"), Bytes.toBytes(t._2))
//      (new ImmutableBytesWritable(), put)
//    }).saveAsHadoopDataset(job)
//    val storingTime = System.currentTimeMillis() - time
//    Thread.sleep(1000)
//    time = System.currentTimeMillis()
//    val result = tmp2.map(t => (t._1, Geometries.rectangleGeographic(t._3.getMinX, t._3.getMinY, t._3.getMaxX, t._3.getMaxY))).collectAsMap()
//    var rTree = new RTree[java.lang.Long, Rectangle]
//    for (elem <- result) {
//      rTree = rTree.add(elem._1, elem._2)
//    }
//    val indexingTime = System.currentTimeMillis() - time
//    for (elem <- queryMBRs) {
//      val mbr = elem.replaceAll(" ", "").split(",")
//      val gets: util.List[Get] = new util.ArrayList[Get]
//      var ranges = rTree.search(Geometries.rectangleGeographic(mbr(0).toDouble, mbr(1).toDouble, mbr(2).toDouble, mbr(3).toDouble))
//
//    }
//  }
}
