package com.just.ksim.filter;

// cc CustomFilterExample Example using a custom filter

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String name = "testtable2";
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable(name);
    helper.createTable(name, "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable(name, 1, 10, 10, 2, true, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf(name));
    // vv CustomFilterExample
    List<Filter> filters = new ArrayList<Filter>();

    Filter filter1 = new CustomFilter(Bytes.toBytes("val-05.05"));
    filters.add(filter1);

    Filter filter2 = new CustomFilter(Bytes.toBytes("val-02.07"));
    filters.add(filter2);

    Filter filter3 = new CustomFilter(Bytes.toBytes("val-09.01"));
    filters.add(filter3);

    FilterList filterList = new FilterList(
      FilterList.Operator.MUST_PASS_ONE, filters);

    Scan scan = new Scan();
    scan.setFilter(filterList);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ CustomFilterExample
    System.out.println("Results of scan:");
    // vv CustomFilterExample
    for (Result result : scanner) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }
    }
    scanner.close();
    table.close();
    // ^^ CustomFilterExample
  }
}
