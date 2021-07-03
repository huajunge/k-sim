package com.just.ksim.disks;

import com.google.common.collect.MinMaxPriorityQueue;
import com.just.ksim.entity.Trajectory;
import com.just.ksim.filter.CalculateSimilarity;
import com.just.ksim.filter.PivotsFilter;
import com.just.ksim.filter.SimilarityFilterCount;
import com.just.ksim.filter.WithoutMemoryFilter;
import com.just.ksim.index.ElementKNN;
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
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.sfcurve.IndexRange;
import scala.Tuple2;
import utils.WKTUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static utils.Constants.*;

/**
 * @author : hehuajun3
 * @description : Client
 * @date : Created in 2021-03-09 17:09
 * @modified by :
 **/
public class KNNClient {

    private short g;
    private int beta = 1;
    private XZStarSFC sfc;
    private Admin admin;
    private HTable hTable;
    private String tableName;
    private static int MAX_ITERTOR = 100;
    private Connection connection;
    private Short shard = 4;
//    public Client(short g) {
//        this.g = g;
//        this.sfc = XZStarSFC.apply(g, beta);
//    }

    public KNNClient(String tableName) throws IOException {
        this(tableName, DEFALUT_G);
    }

    public KNNClient(short precise, String tableName, Short shard) throws IOException {
        this.shard = shard;
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

    public KNNClient(String tableName, Short precise) throws IOException {
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
        table.addFamily(new HColumnDescriptor(DEFAULT_CF));
        this.admin.createTable(table);
    }

    public void knnQuery() {

    }
    public void insert(Trajectory traj) throws IOException {
        hTable.put(getPut(traj));
    }

    public interface ResultProcess {
        void process(Result result);
    }

    public Put getPut(Trajectory traj) {
        String id = traj.getId();
        long index = sfc.index(traj.getMultiPoint(), false);
        short s = (short) (index % shard);
        byte[] bytes = new byte[9 + id.length()];
        bytes[0] = (byte) s;
        ByteArrays.writeLong(index, bytes, 1);
        System.arraycopy(Bytes.toBytes(id), 0, bytes, 9, id.length());
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID), Bytes.toBytes(id));
        StringBuilder indexString = new StringBuilder();
        for (Integer integer : traj.getDPFeature().getIndexes()) {
            indexString.append(integer).append(",");
        }
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(PIVOT), Bytes.toBytes(traj.getDPFeature().getMBRs().toText() + "--" + indexString.toString()));
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
