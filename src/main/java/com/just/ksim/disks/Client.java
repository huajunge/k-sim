package com.just.ksim.disks;

import com.google.common.collect.MinMaxPriorityQueue;
import com.just.ksim.entity.Trajectory;
import com.just.ksim.filter.CalculateSimilarity;
import com.just.ksim.filter.PivotsFilter;
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
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.sfcurve.IndexRange;
import scala.Tuple2;
import util.WKTUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static int MAX_ITERTOR = 100;
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
        table.addFamily(new HColumnDescriptor(DEFAULT_CF));
        this.admin.createTable(table);
    }

    public void insert(Trajectory traj) throws IOException {
        hTable.put(getPut(traj));
    }

    public List<Trajectory> simQuery(Trajectory traj, double threshold) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        List<Trajectory> trajectories = new ArrayList<>();
        query(sfc.simRange(traj, threshold), this.hTable, filter, res -> {
            Geometry geo = WKTUtils.read(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
            String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
            trajectories.add(new Trajectory(id, (MultiPoint) geo));
            //System.out.println());
        });
        return trajectories;
    }

    public void knnQuery(Trajectory traj, int k) throws IOException {
        final ArrayList<IndexRange> queied = new ArrayList<>();
        double threshold;
        double interval = 0.002;
        AtomicInteger currentSize = new AtomicInteger();
        List<Filter> filter = new ArrayList<>(2);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        filter.add(new CalculateSimilarity(traj.toText()));
        for (int iter = 0; iter < MAX_ITERTOR; iter++) {
            threshold = interval * (double) iter;
            long time = System.currentTimeMillis();
            List<IndexRange> ranges = sfc.kNNRanges(traj, threshold, queied);
            System.out.println("iter:" + iter + ",threshold:" + threshold + ",ranges time:" + (System.currentTimeMillis() - time) + ",size" + ranges.size() + ",queried size" + queied.size());
            time = System.currentTimeMillis();
            query(ranges, hTable, filter, res -> {
                currentSize.getAndIncrement();
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID))));
            });
            //System.out.println("query:" + (System.currentTimeMillis()  - time));
            time = System.currentTimeMillis();
            if (currentSize.get() >= k) {
                Filter filter2 = new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null, true);
                List<IndexRange> ranges2 = sfc.kNNRanges(traj, threshold, queied);
                query(ranges2, hTable, Collections.singletonList(filter2), res -> {
                    currentSize.getAndIncrement();
                });
                break;
            }
        }
    }

    public MinMaxPriorityQueue<Tuple2<Trajectory, Double>> knnQuery2(Trajectory traj, int k) throws IOException {
        double threshold;
        double interval = 0.002;
        AtomicInteger currentSize = new AtomicInteger();
        List<Filter> filter = new ArrayList<>(2);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        filter.add(new CalculateSimilarity(traj.toText()));
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        MinMaxPriorityQueue<Tuple2<Trajectory, Double>> tmpResult = MinMaxPriorityQueue.orderedBy(new Comparator<Tuple2<Trajectory, Double>>() {
            @Override
            public int compare(Tuple2<Trajectory, Double> o1, Tuple2<Trajectory, Double> o2) {
                return Double.compare(o1._2, o2._2);
            }
        }).maximumSize(k).create();

        for (int iter = 0; iter < MAX_ITERTOR; iter++) {
            threshold = interval * (double) iter;
            //long time = System.currentTimeMillis();
            List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
            //System.out.println("iter:" + iter + ",threshold:" + threshold + ",ranges time:" + (System.currentTimeMillis() - time) + ",size" + ranges.size());
            List<Tuple2<Trajectory, Double>> tmpTrajs = new ArrayList<>();
            query(ranges, hTable, filter, res -> {
                currentSize.getAndIncrement();
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID))));
                String[] values = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))).split("-");
                Geometry geo = WKTUtils.read(values[0]);
                String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), Double.valueOf(values[1])));
            });
            if (!tmpTrajs.isEmpty()) {
                tmpResult.addAll(tmpTrajs);
            }
            //System.out.println("query:" + (System.currentTimeMillis()  - time));
            if (currentSize.get() >= k) {
                List<Tuple2<Trajectory, Double>> tmpTrajs2 = new ArrayList<>();
                threshold = tmpResult.peek()._2;
                Filter filter2 = new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null, true);
                List<IndexRange> ranges2 = sfc.rangesForKnn(traj, threshold, root);
                query(ranges2, hTable, Collections.singletonList(filter2), res -> {
                    currentSize.getAndIncrement();
                    String[] values = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))).split("-");
                    Geometry geo = WKTUtils.read(values[0]);
                    String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                    tmpTrajs2.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), Double.valueOf(values[1])));
                });
                if (!tmpTrajs2.isEmpty()) {
                    tmpResult.addAll(tmpTrajs2);
                }
                break;
            }
        }
        return tmpResult;
    }

    public interface ResultProcess {
        void process(Result result);
    }

    public void query(List<IndexRange> ranges, HTable table, List<Filter> filter, ResultProcess process) throws IOException {
        if (ranges.isEmpty()) {
            return;
        }
        final List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        for (IndexRange a : ranges) {
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
        List<Filter> filters = new ArrayList<>(2);
        filters.add(new MultiRowRangeFilter(rowRanges));
        //filters.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        if (null != filter) {
            filters.addAll(filter);
        }
        FilterList filterList = new FilterList(filters);
        scan.setFilter(filterList);
        //这里计算
        ResultScanner resultScanner = null;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert resultScanner != null;
        for (Result res : resultScanner) {
            process.process(res);
        }
        resultScanner.close();
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
