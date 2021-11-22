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
import org.apache.hadoop.hbase.filter.*;
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
import java.util.Collections;
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
public class Client {

    private short g;
    private int beta = 1;
    private XZStarSFC sfc;
    private Admin admin;
    private Table hTable;
    private String tableName;
    private static int MAX_ITERTOR = 100;
    private Connection connection;
    private Short shard = 4;
//    public Client(short g) {
//        this.g = g;
//        this.sfc = XZStarSFC.apply(g, beta);
//    }

    public Client(String tableName) throws IOException {
        this(tableName, DEFALUT_G);
    }

    public Client(short precise, String tableName, Short shard) throws IOException {
        this.shard = shard;
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.tableName = tableName;
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        if (!admin.tableExists(table.getTableName())) {
            create();
        }
        //TableBuilderBase base = ;
        this.hTable = connection.getTable(TableName.valueOf(tableName));
        //this.hTable = new HTable(TableName.valueOf(tableName), connection);
        this.g = precise;
        this.sfc = XZStarSFC.apply(g, beta);
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
        this.hTable = connection.getTable(TableName.valueOf(tableName));
        //this.hTable = new HTable(TableName.valueOf(tableName), connection);
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

    public List<Trajectory> limit(int n) throws IOException {
        Scan scan = new Scan();
        scan.setMaxResultSize(n);
        scan.setLimit(n);
        ResultScanner resultScanner = hTable.getScanner(scan);
        List<Trajectory> trajectories = new ArrayList<>();
//        val trajectories: util.List[Trajectory] = new util.ArrayList[Trajectory]
        for (Result res : resultScanner) {
            Geometry geo = WKTUtils.read(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
            String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
            trajectories.add(new Trajectory(id, (MultiPoint) geo));
        }
        return trajectories;
    }

    public List<Trajectory> simQuery(Trajectory traj, double threshold) throws IOException {
        return simQuery(traj, threshold, 0);
    }

    public int  rangeQuery(double lat1, double lng1, double lat2, double lng2) throws IOException {
        List<IndexRange> ranges = sfc.ranges(lat1, lng1, lat2, lng2);
        List<Trajectory> trajectories = new ArrayList<>();
        AtomicInteger integer=new AtomicInteger(0);
        query(ranges, this.hTable, Collections.singletonList(new FirstKeyOnlyFilter()), res -> {
//            String v = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM)));
//            try {
//                Geometry geo = WKTUtils.read(v);
//                String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
//                trajectories.add(new Trajectory(id, (MultiPoint) geo));
//            } catch (Exception ignored) {
//
//            }
            integer.incrementAndGet();
            //System.out.println());
        });
        return integer.get();
    }

    public List<Trajectory> simQuery(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), false));
        filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, false));
        //filter.add(new SimilarityFilter(traj.toText(), threshold));
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        query(ranges, this.hTable, filter, res -> {
            String v = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM)));
            try {
                Geometry geo = WKTUtils.read(v);
                String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                trajectories.add(new Trajectory(id, (MultiPoint) geo));
            } catch (Exception ignored) {

            }

            //System.out.println());
        });
        return trajectories;
    }

    public List<Trajectory> xz2SimQuery(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), false));
        filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, false));
        //filter.add(new SimilarityFilter(traj.toText(), threshold));
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.xz2RangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        query(ranges, this.hTable, filter, res -> {
            String v = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM)));
            try {
                Geometry geo = WKTUtils.read(v);
                String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                trajectories.add(new Trajectory(id, (MultiPoint) geo));
            } catch (Exception ignored) {

            }

            //System.out.println());
        });
        return trajectories;
    }

    public List<Trajectory> quadSimQuery(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), false));
        filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, false));
        //filter.add(new SimilarityFilter(traj.toText(), threshold));
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.quadRangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        query(ranges, this.hTable, filter, res -> {
            String v = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM)));
            try {
                Geometry geo = WKTUtils.read(v);
                String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                trajectories.add(new Trajectory(id, (MultiPoint) geo));
            } catch (Exception ignored) {

            }
            //System.out.println());
        });
        return trajectories;
    }

    public List<Trajectory> simQueryWithoutMemory(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), false));
        filter.add(new WithoutMemoryFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, false));
        //filter.add(new SimilarityFilter(traj.toText(), threshold));
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        query(ranges, this.hTable, filter, res -> {
            String v = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM)));
            try {
                Geometry geo = WKTUtils.read(v);
                String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                trajectories.add(new Trajectory(id, (MultiPoint) geo));
            } catch (Exception ignored) {

            }

            //System.out.println());
        });
        return trajectories;
    }

    public int simQueryCount2(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        //filter.add(new SimilarityFilterCount(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, false));
        filter.add(new FirstKeyOnlyFilter());
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        AtomicInteger integer = new AtomicInteger(0);
        query(ranges, this.hTable, filter, res -> {
            integer.incrementAndGet();
            //System.out.println());
        });
        return integer.get();
    }

    public int simQueryCount(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        filter.add(new SimilarityFilterCount(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, false));
        //filter.add(new FirstKeyOnlyFilter());
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        AtomicInteger integer = new AtomicInteger(0);
        query(ranges, this.hTable, filter, res -> {
            integer.incrementAndGet();
            //System.out.println());
        });
        return integer.get();
    }

    public int simQueryCountWithoutMemory(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        filter.add(new FirstKeyOnlyFilter()); //filter.add(new FirstKeyOnlyFilter());
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        AtomicInteger integer = new AtomicInteger(0);
        query(ranges, this.hTable, filter, res -> {
            integer.incrementAndGet();
            //System.out.println());
        });
        return integer.get();
    }

    public int xzSimQueryCount(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        filter.add(new SimilarityFilterCount(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, false));
        //filter.add(new FirstKeyOnlyFilter());
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.xz2RangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        AtomicInteger integer = new AtomicInteger(0);
        query(ranges, this.hTable, filter, res -> {
            integer.incrementAndGet();
            //System.out.println());
        });
        return integer.get();
    }

    public int xzSimQueryCountWithOutMemory(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        filter.add(new FirstKeyOnlyFilter());
        //filter.add(new FirstKeyOnlyFilter());
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.xz2RangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        AtomicInteger integer = new AtomicInteger(0);
        query(ranges, this.hTable, filter, res -> {
            integer.incrementAndGet();
            //System.out.println());
        });
        return integer.get();
    }

    public int xzSimQueryCountWithOutMemory2(Trajectory traj, double threshold, int disFunc) throws IOException {
        List<Filter> filter = new ArrayList<>(2);
        filter.add(new FirstKeyOnlyFilter());
        //filter.add(new FirstKeyOnlyFilter());
        List<Trajectory> trajectories = new ArrayList<>();
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        List<IndexRange> ranges = sfc.xz2RangesForKnn(traj, threshold, root);
        //List<IndexRange> ranges = sfc.simRange(traj, threshold);

        //sfc.simRange(traj, threshold)
        AtomicInteger integer = new AtomicInteger(0);
        query(ranges, this.hTable, filter, res -> {
            integer.incrementAndGet();
            //System.out.println());
        });
        return integer.get();
    }

    public int simQueryCount(Trajectory traj, double threshold) throws IOException {
        return simQueryCount(traj, threshold, 0);
    }

    public MinMaxPriorityQueue<Tuple2<Trajectory, Double>> knnQuery(Trajectory traj, int k, Double interval) throws IOException {
        return knnQuery(traj, k, interval, 0);
    }

    public MinMaxPriorityQueue<Tuple2<Trajectory, Double>> knnQuery(Trajectory traj, int k, Double interval, int disFunc) throws IOException {
        double threshold;
        double currentThreshold = Double.MAX_VALUE;
        //double interval = 0.002;
        AtomicInteger currentSize = new AtomicInteger();
        List<Filter> filter = new ArrayList<>(1);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        filter.add(new CalculateSimilarity(traj.toText(), disFunc));
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        MinMaxPriorityQueue<Tuple2<Trajectory, Double>> tmpResult = MinMaxPriorityQueue.orderedBy((Comparator<Tuple2<Trajectory, Double>>) (o1, o2) -> Double.compare(o1._2, o2._2)).maximumSize(k).create();
        int iter = 0;
        double c = 1;
        for (; iter <= MAX_ITERTOR; iter++) {
            threshold = interval * (double) iter;
            //long time = System.currentTimeMillis();
            List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
            //System.out.println("iter:" + iter + ",threshold:" + threshold + ",ranges time:" + (System.currentTimeMillis() - time) + ",size" + ranges.size());
            List<Tuple2<Trajectory, Double>> tmpTrajs = new ArrayList<>();
            // System.out.println(ranges.size());
            if (currentSize.get() >= k) {
                double maxThreshold = tmpResult.peekLast()._2;
                Filter filterThreshold = new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), maxThreshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, true);
                filter.clear();
                //filter.add(new CalculateSimilarity(traj.toText()));
                filter.add(filterThreshold);
                //filter.add(new SimilarityFilter(traj.toText(), maxThreshold, true));
            }
            query(ranges, hTable, filter, res -> {
                currentSize.getAndIncrement();
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID))));
                try {
                    String[] values = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))).split("-");
                    Geometry geo = WKTUtils.read(values[0]);
                    String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                    //System.out.println(id);
                    if (values.length == 2) {
                        BigDecimal d = new BigDecimal(values[1]);
                        tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
                    }
                } catch (Exception ignored) {

                }

//                BigDecimal d = new BigDecimal(values[1]);
//                tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
            });
//            if(!ranges.isEmpty()) {
//                final KNNServer.KnnRequest request = KNNServer.KnnRequest.newBuilder()
//                        .setFunc(disFunc)
//                        .setK(k)
//                        .setThreshold(currentThreshold)
//                        .setMbrs(traj.getDPFeature().getMBRs().toText())
//                        .addAllPivots(traj.getDPFeature().getIndexes())
//                        .setTraj(traj.toText())
//                        .addAllRanges(ranges.stream().map(v -> KNNServer.Range.newBuilder().setStart(v.lower()).setEnd(v.upper() + 1L).build()).collect(Collectors.toList()))
//                        .build();
//                Map<byte[], KNNServer.KnnResponse> result = null;
//                try {
//                    result = hTable.coprocessorService(KNNServer.KnnService.class, null, null, instance -> {
//                        BlockingRpcCallback<KNNServer.KnnResponse> rpcCallback =
//                                new BlockingRpcCallback<>();
//                        instance.getTopK(null, request, rpcCallback);
//                        return rpcCallback.get();
//                    });
//                } catch (Throwable e) {
//                    e.printStackTrace();
//                }
//                for (KNNServer.KnnResponse value : result.values()) {
//                    for (KNNServer.Traj traj1 : value.getTrajsList()) {
//                        Geometry geo = WKTUtils.read(traj1.getTrajGeom());
//                        tmpResult.add(new Tuple2<>(new Trajectory("", (MultiPoint) geo), (double) traj1.getSim()));
//                        currentSize.getAndIncrement();
//                    }
//                }
//            }


            if (!tmpTrajs.isEmpty()) {
                tmpResult.addAll(tmpTrajs);
            }

            if (currentSize.get() >= k) {
                double maxThreshold = tmpResult.peekLast()._2;
                currentThreshold = maxThreshold;
                //System.out.println(maxThreshold);
                if (maxThreshold <= threshold) {
                    break;
                }
            }
        }
        return tmpResult;
    }

    public int knnQueryCount(Trajectory traj, int k, Double interval) throws IOException {
        return knnQueryCount(traj, k, interval, 0);
    }

    public int knnQueryCount(Trajectory traj, int k, Double interval, int disFunc) throws IOException {
        double threshold;
        //double interval = 0.002;
        AtomicInteger currentSize = new AtomicInteger();
        List<Filter> filter = new ArrayList<>(1);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        filter.add(new CalculateSimilarity(traj.toText(), disFunc));
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        MinMaxPriorityQueue<Tuple2<Trajectory, Double>> tmpResult = MinMaxPriorityQueue.orderedBy((Comparator<Tuple2<Trajectory, Double>>) (o1, o2) -> Double.compare(o1._2, o2._2)).maximumSize(k).create();
        int iter = 0;
        double c = 1;
        AtomicInteger count = new AtomicInteger(0);
        for (; iter <= MAX_ITERTOR; iter++) {
            threshold = interval * (double) iter;
            //long time = System.currentTimeMillis();
            List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
            //System.out.println("iter:" + iter + ",threshold:" + threshold + ",ranges time:" + (System.currentTimeMillis() - time) + ",size" + ranges.size());
            List<Tuple2<Trajectory, Double>> tmpTrajs = new ArrayList<>();
            // System.out.println(ranges.size());
            if (currentSize.get() >= k) {
                double maxThreshold = tmpResult.peekLast()._2;
                Filter filterThreshold = new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), maxThreshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, true);
                filter.clear();
                //filter.add(new CalculateSimilarity(traj.toText()));
                filter.add(filterThreshold);
                List<Filter> filter2 = new ArrayList<>(1);
                filter2.add(new SimilarityFilterCount(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, false));
                //filter2.add(new FirstKeyOnlyFilter());
                query(ranges, this.hTable, filter2, res -> {
                    count.incrementAndGet();
                    //System.out.println());
                });
                //filter.add(new SimilarityFilter(traj.toText(), maxThreshold, true));
            }
            query(ranges, hTable, filter, res -> {
                if (currentSize.get() < k) {
                    count.incrementAndGet();
                }
                currentSize.getAndIncrement();
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID))));
                try {
                    String[] values = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))).split("-");
                    Geometry geo = WKTUtils.read(values[0]);
                    String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                    //System.out.println(id);
                    if (values.length == 2) {
                        BigDecimal d = new BigDecimal(values[1]);
                        tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
                    }
                } catch (Exception ignored) {

                }

//                BigDecimal d = new BigDecimal(values[1]);
//                tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
            });

            if (!tmpTrajs.isEmpty()) {
                tmpResult.addAll(tmpTrajs);
            }

            if (currentSize.get() >= k) {
                double maxThreshold = tmpResult.peekLast()._2;
                //System.out.println(maxThreshold);
                if (maxThreshold <= threshold) {
                    break;
                }
            }
        }
        return count.get();
    }

    public int xz2knnQueryCount(Trajectory traj, int k, Double interval, int disFunc) throws IOException {
        double threshold;
        //double interval = 0.002;
        AtomicInteger currentSize = new AtomicInteger();
        List<Filter> filter = new ArrayList<>(1);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        filter.add(new CalculateSimilarity(traj.toText(), disFunc));
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        MinMaxPriorityQueue<Tuple2<Trajectory, Double>> tmpResult = MinMaxPriorityQueue.orderedBy((Comparator<Tuple2<Trajectory, Double>>) (o1, o2) -> Double.compare(o1._2, o2._2)).maximumSize(k).create();
        int iter = 0;
        double c = 1;
        AtomicInteger count = new AtomicInteger(0);
        for (; iter <= MAX_ITERTOR; iter++) {
            threshold = interval * (double) iter;
            //long time = System.currentTimeMillis();
            List<IndexRange> ranges = sfc.xz2RangesForKnn(traj, threshold, root);
            //System.out.println("iter:" + iter + ",threshold:" + threshold + ",ranges time:" + (System.currentTimeMillis() - time) + ",size" + ranges.size());
            List<Tuple2<Trajectory, Double>> tmpTrajs = new ArrayList<>();
            // System.out.println(ranges.size());
            if (currentSize.get() >= k) {
                double maxThreshold = tmpResult.peekLast()._2;
                Filter filterThreshold = new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), maxThreshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, true);
                filter.clear();
                //filter.add(new CalculateSimilarity(traj.toText()));
                filter.add(filterThreshold);
//                List<Filter> filter2 = new ArrayList<>(1);
//                filter2.add(new FirstKeyOnlyFilter());//filter2.add(new FirstKeyOnlyFilter());
//                query(ranges, this.hTable, filter2, res -> {
//                    count.incrementAndGet();
//                    //System.out.println());
//                });
                //filter.add(new SimilarityFilter(traj.toText(), maxThreshold, true));
            }
//            List<Filter> filter2 = new ArrayList<>(1);
//            filter2.add(new FirstKeyOnlyFilter());//filter2.add(new FirstKeyOnlyFilter());
//            query(ranges, this.hTable, filter2, res -> {
//                count.incrementAndGet();
//                //System.out.println());
//            });
            query(ranges, hTable, filter, res -> {
//                if (currentSize.get() < k) {
//                    count.incrementAndGet();
//                }
                currentSize.getAndIncrement();
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID))));
                try {
                    String[] values = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))).split("-");
                    Geometry geo = WKTUtils.read(values[0]);
                    String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                    //System.out.println(id);
                    if (values.length == 2) {
                        BigDecimal d = new BigDecimal(values[1]);
                        tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
                    }
                } catch (Exception ignored) {

                }

//                BigDecimal d = new BigDecimal(values[1]);
//                tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
            });

            if (!tmpTrajs.isEmpty()) {
                tmpResult.addAll(tmpTrajs);
            }

            if (currentSize.get() >= k) {
                double maxThreshold = tmpResult.peekLast()._2;
                //System.out.println(maxThreshold);
                if (maxThreshold <= threshold) {
                    break;
                }
            }
        }
        return count.get();
    }

    public int knnQueryCountWithoutM(Trajectory traj, int k, Double interval, int disFunc) throws IOException {
        double threshold;
        //double interval = 0.002;
        AtomicInteger currentSize = new AtomicInteger();
        List<Filter> filter = new ArrayList<>(1);
        //filter.add(new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), threshold, traj.toText(), null));
        filter.add(new CalculateSimilarity(traj.toText(), disFunc));
        final ElementKNN root = new ElementKNN(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L);
        MinMaxPriorityQueue<Tuple2<Trajectory, Double>> tmpResult = MinMaxPriorityQueue.orderedBy((Comparator<Tuple2<Trajectory, Double>>) (o1, o2) -> Double.compare(o1._2, o2._2)).maximumSize(k).create();
        int iter = 0;
        double c = 1;
        AtomicInteger count = new AtomicInteger(0);
        for (; iter <= MAX_ITERTOR; iter++) {
            threshold = interval * (double) iter;
            //long time = System.currentTimeMillis();
            List<IndexRange> ranges = sfc.rangesForKnn(traj, threshold, root);
            //System.out.println("iter:" + iter + ",threshold:" + threshold + ",ranges time:" + (System.currentTimeMillis() - time) + ",size" + ranges.size());
            List<Tuple2<Trajectory, Double>> tmpTrajs = new ArrayList<>();
            // System.out.println(ranges.size());
            if (currentSize.get() >= k) {
                double maxThreshold = tmpResult.peekLast()._2;
                Filter filterThreshold = new PivotsFilter(traj.getGeometryN(0).toText(), traj.getGeometryN(traj.getNumGeometries() - 1).toText(), maxThreshold, traj.toText(), traj.getDPFeature().getIndexes(), traj.getDPFeature().getMBRs().toText(), disFunc, true);
                filter.clear();
                //filter.add(new CalculateSimilarity(traj.toText()));
                filter.add(filterThreshold);
//                List<Filter> filter2 = new ArrayList<>(1);
//                filter2.add(new FirstKeyOnlyFilter());//filter2.add(new FirstKeyOnlyFilter());
//                query(ranges, this.hTable, filter2, res -> {
//                    count.incrementAndGet();
//                    //System.out.println());
//                });
                //filter.add(new SimilarityFilter(traj.toText(), maxThreshold, true));
            }

            query(ranges, hTable, filter, res -> {
//                if (currentSize.get() < k) {
//                    count.incrementAndGet();
//                }
                currentSize.getAndIncrement();
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))));
                //System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID))));
                try {
                    String[] values = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))).split("-");
                    Geometry geo = WKTUtils.read(values[0]);
                    String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
                    //System.out.println(id);
                    if (values.length == 2) {
                        BigDecimal d = new BigDecimal(values[1]);
                        tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
                    }
                } catch (Exception ignored) {

                }

//                BigDecimal d = new BigDecimal(values[1]);
//                tmpTrajs.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
            });

            if (!tmpTrajs.isEmpty()) {
                tmpResult.addAll(tmpTrajs);
            }

            if (currentSize.get() >= k) {
                double maxThreshold = tmpResult.peekLast()._2;
                //System.out.println(maxThreshold);
                if (maxThreshold <= threshold) {
                    break;
                }
            }
            List<Filter> filter2 = new ArrayList<>(1);
            filter2.add(new FirstKeyOnlyFilter());//filter2.add(new FirstKeyOnlyFilter());
            query(ranges, this.hTable, filter2, res -> {
                count.incrementAndGet();
                //System.out.println());
            });
        }
        return count.get();
    }

    public interface ResultProcess {
        void process(Result result);
    }

    public void query(List<IndexRange> ranges, Table table, List<Filter> filter, ResultProcess process) throws IOException {
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
//        scan.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID));
//        scan.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(START_POINT));
//        scan.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(END_POINT));
//        scan.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM));
        //这里计算
        ResultScanner resultScanner = null;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //assert resultScanner != null;
        if (null != resultScanner) {
            for (Result res : resultScanner) {
                process.process(res);
            }
            resultScanner.close();
        }
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
