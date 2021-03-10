package com.just.ksim.disks;

import com.just.ksim.entity.Trajectory;
import com.just.ksim.filter.PivotsFilter;
import com.just.ksim.index.XZStarSFC;
import com.just.ksim.utils.ByteArrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.sfcurve.IndexRange;
import util.WKTUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static util.Constants.*;

/**
 * @author : hehuajun3
 * @description : Client
 * @date : Created in 2021-03-09 17:09
 * @modified by :
 **/
public class Client {

    private short g;
    private int beta = 1;
    private XZStarSFC sfc;
    private Admin admin;
    private HTable hTable;
    private String tableName;

    private Connection connection;
    private static Short shard = 4;
//    public Client(short g) {
//        this.g = g;
//        this.sfc = XZStarSFC.apply(g, beta);
//    }

    public Client(String tableName) throws IOException {
        this(tableName, DEFALUT_G);
    }

    public Client(String tableName, Short precise) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.tableName = tableName;
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        if (!admin.tableExists(table.getTableName())) {
            create();
        }
        this.hTable = new HTable(TableName.valueOf(tableName), connection);
        this.g = precise;
        this.sfc = XZStarSFC.apply(g, beta);
    }

    public void create() throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(this.tableName));
        if (this.admin.tableExists(table.getTableName())) {
            this.admin.disableTable(table.getTableName());
            this.admin.deleteTable(table.getTableName());
        }
        table.addFamily(new HColumnDescriptor(DEFAULT_CF).setCompressionType(Compression.Algorithm.SNAPPY));
        this.admin.createTable(table);
    }

    public void insert(Trajectory traj) throws IOException {
        hTable.put(getPut(traj));
    }

    public void query(Trajectory traj, double threshold) throws IOException {
        final List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();

        for (IndexRange a : sfc.simRange(traj, threshold)) {
            for (int i = 0; i < shard; i++) {
                byte[] startRow = new byte[9];
                byte[] endRow = new byte[9];
                startRow[0] = (byte) i;
                endRow[0] = (byte) i;
                ByteArrays.writeLong(a.lower(), startRow, 1);
                ByteArrays.writeLong(a.upper() + 1L, endRow, 1);
                rowRanges.add(new MultiRowRangeFilter.RowRange(startRow, true, endRow, true));
            }
        }
        Scan scan = new Scan();
        scan.setCaching(1000);
        //scan.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL));
        List<Filter> filters = new ArrayList<>(2);
        filters.add(new MultiRowRangeFilter(rowRanges));
        filters.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        FilterList filterList = new FilterList(filters);
        scan.setFilter(filterList);
        //scan.setFilter(new MultiRowRangeFilter(rowRanges));
        //scan.setFilter(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        //这里计算
        try (HTable table = new HTable(TableName.valueOf(tableName), connection)) {
            ResultScanner resultScanner = null;
            try {
                resultScanner = table.getScanner(scan);
            } catch (IOException e) {
                e.printStackTrace();
            }
            assert resultScanner != null;
            for (Result res : resultScanner) {
                Geometry geo = WKTUtils.read(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
                System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID))));
                //resultList.add(res);
            }
            resultScanner.close();
        }
    }

    public Put getPut(Trajectory traj) {
        String id = traj.getId();
        long index = sfc.index(traj, false);
        short s = (short) (index % shard);
        byte[] bytes = new byte[9 + id.length()];
        bytes[0] = (byte) s;
        ByteArrays.writeLong(index, bytes, 1);
        System.arraycopy(Bytes.toBytes(id), 0, bytes, 9, id.length());
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID), Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM), Bytes.toBytes(traj.toText()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(START_POINT), Bytes.toBytes(traj.getGeometryN(0).toText()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(END_POINT), Bytes.toBytes(traj.getGeometryN(traj.getNumGeometries() - 1).toText()));
        return put;
    }

    public void close() throws IOException {
        admin.close();
        hTable.close();
        connection.close();
    }
}
