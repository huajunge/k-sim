package com.just.ksim.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseHelper;

import java.io.IOException;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-01-31 16:16
 * @modified by :
 **/
public class RegionObserverTest {
    public static void main(String[] args) throws IOException {
        TableName tableName = TableName.valueOf("testtable3");

        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable3");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        HTableDescriptor htd = new HTableDescriptor(tableName); // co LoadWithTableDescriptorExample-1-Define Define a table descriptor.
        htd.addFamily(new HColumnDescriptor("colfam1"));
        htd.addFamily(new HColumnDescriptor("colfam2"));
//        htd.setValue("COPROCESSOR$1", "D:/hbase-1.6.0/lib/k-sim-traj-1.0-SNAPSHOT.jar|" +
//                RegionObserverExample.class.getCanonicalName() +
//                "|" + Coprocessor.PRIORITY_USER);

        admin.createTable(htd);

        System.out.println(admin.getTableDescriptor(tableName));


        // ^^ EndpointExample
        //helper.dropTable("testtable");
        //helper.createTable("testtable", "colfam1", "colfam2");
        helper.put("testtable",
                new String[]{"row1", "row2", "row3", "row4", "row5"},
                new String[]{"colfam1", "colfam2"},
                new String[]{"qual1", "qual1"},
                new long[]{1, 2},
                new String[]{"val1", "val2"});
        helper.dump("testtable",
                new String[]{"row1", "row2", "row3", "row4", "row5"},
                null, null);
        try {
            admin.split(tableName, Bytes.toBytes("row3"));
        } catch (IOException e) {
            e.printStackTrace();
        }
//         wait for the split to be done
        while (admin.getTableRegions(tableName).size() < 2) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        System.out.println("start---------"); // co ScanExample-3-Dump Print row content.

        //vv EndpointExample
        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        Result ress = results.next();
        System.out.println(ress);
        for (Result res : results) {
            System.out.println(res); // co ScanExample-3-Dump Print row content.
        }
        admin.close();
        connection.close();
    }
}
