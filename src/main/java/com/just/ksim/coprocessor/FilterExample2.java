package com.just.ksim.coprocessor;

// cc CustomFilterExample Example using a custom filter

import com.just.ksim.filter.CustomFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterExample2 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String name = "testtable111";
    TableName tableName = TableName.valueOf(name);

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable(name);
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    HTableDescriptor htd = new HTableDescriptor(tableName); // co LoadWithTableDescriptorExample-1-Define Define a table descriptor.
    htd.addFamily(new HColumnDescriptor("colfam1"));
    //htd.addFamily(new HColumnDescriptor("colfam2"));
//    htd.setValue("COPROCESSOR$1", "D:/hbase-1.6.0/lib/k-sim-traj-1.0-SNAPSHOT.jar|" +
//            RegionO bserverExample2.class.getCanonicalName() +
//            "|1001");
    Path path = new Path("file:///D:/hbase-1.6.0/k-sim-traj-1.0-SNAPSHOT12.jar");
    htd.addCoprocessor(RegionObserverExample7.class.getCanonicalName(),
            path, Coprocessor.PRIORITY_USER, null);

    admin.createTable(htd);

    System.out.println(admin.getTableDescriptor(tableName));
    //helper.createTable(name, "colfam1");
    System.out.println("Adding rows to table...");
    //helper.fillTable(name, 1, 10, 10, 2, true, "colfam1");
    helper.fillTable(name, 1, 2, 2, "colfam1");

    Table table = connection.getTable(tableName);
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
                Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
      }
    }
//    Get get = new Get(Bytes.toBytes("row-1"));
//    Result result = table.get(get);
//    System.out.println("result");
//    helper.dumpResult(result);

//    Table table = connection.getTable(TableName.valueOf(name));
//    // vv CustomFilterExample
//    List<Filter> filters = new ArrayList<Filter>();
//
//    Filter filter1 = new CustomFilter(Bytes.toBytes("val-05.05"));
//    filters.add(filter1);
//
//    Filter filter2 = new CustomFilter(Bytes.toBytes("val-02.07"));
//    filters.add(filter2);
//
//    Filter filter3 = new CustomFilter(Bytes.toBytes("val-09.01"));
//    filters.add(filter3);
//
//    FilterList filterList = new FilterList(
//      FilterList.Operator.MUST_PASS_ONE, filters);
//
//    Scan scan = new Scan();
//    scan.setFilter(filterList);
//    ResultScanner scanner = table.getScanner(scan);
//    // ^^ CustomFilterExample
//    System.out.println("Results of scan:");
//    // vv CustomFilterExample
//    for (Result result : scanner) {
//      for (Cell cell : result.rawCells()) {
//        System.out.println("Cell: " + cell + ", Value: " +
//          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
//            cell.getValueLength()));
//      }
//    }
//    scanner.close();
    table.close();
    admin.close();
    connection.close();
    // ^^ CustomFilterExample
  }
}
