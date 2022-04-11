package just.urbancomputing.dao;

import com.google.common.collect.MinMaxPriorityQueue;
import coprocessor.generated.KNNServer;
import just.urbancomputing.entity.Trajectory;
import just.urbancomputing.filter.CalculateSimilarity;
import just.urbancomputing.filter.PivotsFilter;
import just.urbancomputing.index.EnlargedElement;
import just.urbancomputing.index.IndexSpace;
import just.urbancomputing.index.XZStarSFC;
import just.urbancomputing.utils.ByteArrays;
import just.urbancomputing.utils.WKTUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.PrecisionModel;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static just.urbancomputing.utils.Constants.DEFALUT_G;
import static just.urbancomputing.utils.Constants.DEFAULT_CF;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-09-09 17:07
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
    Logger logger = Logger.getLogger("LoggingDemo");

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
        //Path path = new Path("file:///D:/projects/TraSS/target/TraSS-1.0-SNAPSHOT-jar-with-dependencies.jar");
//        table.addFamily(new HColumnDescriptor(DEFAULT_CF));
////        table.addCoprocessor(KNNCoprocessor.class.getCanonicalName(),
////                path, Coprocessor.PRIORITY_USER, null);
//        table.setValue("knn","file:///D:/projects/TraSS/target/TraSS-1.0-SNAPSHOT-jar-with-dependencies.jar" +"|" + KNNCoprocessor.class.getCanonicalName()
//        +"|" + Coprocessor.PRIORITY_USER);
//        connection.getAdmin().modifyTable(TableName.valueOf(tableName), table);

        //this.hTable = new HTable(TableName.valueOf(tableName), connection);
        this.hTable = this.connection.getTable(TableName.valueOf(tableName));

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
        this.hTable = this.connection.getTable(TableName.valueOf(tableName));
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

    public MinMaxPriorityQueue<Tuple2<Trajectory, Double>> knnQuery(Trajectory traj, int k, int disFunc) throws IOException, InterruptedException {
        double currentThreshold = Double.MAX_VALUE;
        int iterTimes = 0;
        int searched = 0;
        int searchedIS = 0;
        int minResolution = 0;
        int maxResolution = g;
        AtomicInteger querySize = new AtomicInteger();
        MinMaxPriorityQueue<Tuple2<EnlargedElement, Double>> elements =
                MinMaxPriorityQueue.orderedBy((Comparator<Tuple2<EnlargedElement, Double>>) (e1, e2) -> {
                            if (e1._2.equals(e2._2)) {
                                int r = Double.compare(e1._1.cellDisToTraj(traj), e2._1.cellDisToTraj(traj));
                                if (r == 0) {
                                    return -Double.compare(e1._1.level(), e2._1.level());
                                }
                                return r;
                            }
                            return Double.compare(e1._2, e2._2);
                        }
                ).create();

        MinMaxPriorityQueue<Tuple2<IndexSpace, Double>> indexSpaces =
                MinMaxPriorityQueue.orderedBy((Comparator<Tuple2<IndexSpace, Double>>) (e1, e2) ->
                {
                    if (e1._2.equals(e2._2)) {
                        return -Double.compare(e1._1.getLevel(), e2._1.getLevel());
                    }
                    return Double.compare(e1._2, e2._2);
                }).create();

        MinMaxPriorityQueue<Tuple2<Trajectory, Double>> results =
                MinMaxPriorityQueue.orderedBy((Comparator<Tuple2<Trajectory, Double>>) (e1, e2) -> Double.compare(e1._2, e2._2)).maximumSize(k).create();

        elements.add(new Tuple2<>(
                new EnlargedElement(-180.0, -90.0, 180.0, 90.0, 0, g, new PrecisionModel(), 0L), 0.0));
        List<Filter> filter = new ArrayList<>(1);
        filter.add(new CalculateSimilarity(traj.toText(), disFunc));
        //final Semaphore semp = new Semaphore(2);
        while (!elements.isEmpty()) {
            iterTimes++;
            //logger.info("开始--elements");
            Tuple2<EnlargedElement, Double> e = elements.poll();
            if (!indexSpaces.isEmpty() && e._2 >= indexSpaces.peek()._2) {
                //logger.info("开始--indexSpaces");
                while (!indexSpaces.isEmpty()) {
                    if (indexSpaces.peek()._2 > e._2) {
                        break;
                    }

                    Tuple2<IndexSpace, Double> is = indexSpaces.poll();
                    if (is._1.getLevel() < minResolution && is._1.getLevel() > maxResolution) {
                        continue;
                    }
                    if (is._2 >= currentThreshold) {
                        System.out.println(currentThreshold + ", " + iterTimes + ", " + searched + ", " + searchedIS);
                        return results;
                    }

                    if (!isFilter(traj, is, currentThreshold)) {
                        if (results.size() == k) {
                            Filter filterThreshold = new PivotsFilter(
                                    traj.getGeometryN(0).toText(),
                                    traj.getGeometryN(traj.getNumGeometries() - 1).toText(),
                                    currentThreshold,
                                    traj.toText(),
                                    traj.getDPFeature().getIndexes(),
                                    traj.getDPFeature().getMBRs().toText(),
                                    disFunc, true);
                            filter.clear();
                            //filter.add(new CalculateSimilarity(traj.toText()));
                            filter.add(filterThreshold);
                        }
                        searched++;
                        List<Tuple2<Long, Long>> ranges = new ArrayList<>(8);
                        ranges.add(new Tuple2<>(is._1.getCode(), is._1.getCode() + 1));
                        while (!indexSpaces.isEmpty() && indexSpaces.peek()._2.equals(is._2)) {
                            long c = indexSpaces.poll()._1.getCode();
                            ranges.add(new Tuple2<>(c, c + 1));
                        }
                        //logger.info("开始--查询");
                        //semp.acquire();
                       /*Runnable run = () -> {

                        };
                        run.run();*/
                        try {
                            //cor
                            if (true) {
                                final KNNServer.KnnRequest request = KNNServer.KnnRequest.newBuilder()
                                        .setFunc(disFunc)
                                        .setK(k)
                                        .setThreshold(currentThreshold)
                                        .setMbrs(traj.getDPFeature().getMBRs().toText())
                                        .addAllPivots(traj.getDPFeature().getIndexes())
                                        .setTraj(traj.toText())
                                        .addAllRanges(ranges.stream().map(v -> KNNServer.Range.newBuilder().setStart(v._1).setEnd(v._2).build()).collect(Collectors.toList()))
                                        .build();
                                Map<byte[], KNNServer.KnnResponse> result = hTable.coprocessorService(KNNServer.KnnService.class, null, null, instance -> {
                                    CoprocessorRpcUtils.BlockingRpcCallback<KNNServer.KnnResponse> rpcCallback =
                                            new CoprocessorRpcUtils.BlockingRpcCallback<>();
                                    instance.getTopK(null, request, rpcCallback);
                                    return rpcCallback.get();
                                });
                                for (KNNServer.KnnResponse value : result.values()) {
                                    for (KNNServer.Traj traj1 : value.getTrajsList()) {
                                        Geometry geo = WKTUtils.read(traj1.getTrajGeom());
                                        results.add(new Tuple2<>(new Trajectory("", (MultiPoint) geo), (double) traj1.getSim()));
                                        querySize.getAndIncrement();
                                    }
                                }
                            }
//                            else {
//                                query(ranges, hTable, filter, res -> {
//                                    try {
//                                        String[] values = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM))).split("-");
//                                        Geometry geo = WKTUtils.read(values[0]);
//                                        String id = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID)));
//                                        //System.out.println(id);
//                                        if (values.length == 2) {
//                                            BigDecimal d = new BigDecimal(values[1]);
//                                            results.add(new Tuple2<>(new Trajectory(id, (MultiPoint) geo), d.doubleValue()));
//                                        }
//                                        querySize.getAndIncrement();
//                                        //semp.release();
//                                    } catch (Exception ignored) {
//                                        //semp.release();
//                                    }
//                                });
//                            }
                        } catch (Throwable ex) {
                            ex.printStackTrace();
                        }
                        //System.out.println("query-- is_dis:" + is._2 + " , cur:" + currentThreshold + " , level:" + is._1.getLevel() + " , size:" + querySize.get());
                        if (results.size() == k) {
                            currentThreshold = results.peekLast()._2;
                            minResolution = Integer.max(sfc.resolution(traj.getXLength() + currentThreshold, 0), sfc.resolution(traj.getYLength() + currentThreshold, 0));
                            maxResolution = Integer.max(sfc.resolution(traj.getXLength() - currentThreshold, 0), sfc.resolution(traj.getYLength() - currentThreshold, 0));
                        }
                        //logger.info("结束--查询");
                    }
                }
                //logger.info("结束--is");
            }
            //logger.info("开始添加--is");
            if (e._2 >= currentThreshold) {
                System.out.println(currentThreshold + ", " + iterTimes + ", " + searched + ", " + searchedIS);
                return results;
            }
            if (!eeFilter(e, currentThreshold, traj)) {
                if (e._1.level() >= minResolution && e._1.level() <= maxResolution) {
                    int invQuads;
                    double[] quadsDis;
                    Tuple2<Object, double[]> quads = e._1.invalidQuads(traj, currentThreshold);
                    invQuads = (int) quads._1;
                    quadsDis = quads._2;
                    for (IndexSpace indexSpace : e._1.indexSpaces()) {
                        if ((invQuads & indexSpace.getSig()) == 0 && !(indexSpace.getLevel() != g && indexSpace.getSig() == 1)) {
                            double quadDis = 0.0;
                            for (int i = 0; i < 4; i++) {
                                if ((indexSpace.getSig() & (1 << i)) != 0) {
                                    quadDis = Double.max(quadsDis[i], quadDis);
                                }
                            }
                            if (quadDis < currentThreshold) {
                                double dx;
                                double dy;
                                if (e._1.xLength() / 2.0 > traj.getXLength()) {
                                    dx = (e._1.xLength() / 2.0 - traj.getXLength()) / 2.0;
                                } else {
                                    dx = (traj.getXLength() - e._1.xLength()) / 2.0;
                                }
                                if (e._1.yLength() / 2.0 > traj.getYLength()) {
                                    dy = (e._1.yLength() / 2.0 - traj.getYLength()) / 2.0;
                                } else {
                                    dy = (traj.getYLength() - e._1.yLength()) / 2.0;
                                }
                                double dis = Double.max(Double.max(indexSpace.dis(traj), quadDis), Double.max(dx,dy));
                                if (dis < currentThreshold) {
                                    searchedIS++;
                                    indexSpaces.add(new Tuple2<>(indexSpace, dis));
                                }
                            }
                        }
                    }
                }
                //判断是否需要index spaces
            }
            //logger.info("结束添加--is");
            //logger.info("开始添加--el");
            if (e._1.level() >= minResolution && e._1.level() <= maxResolution) {
                for (EnlargedElement child : e._1.getChildren()) {
                    double dis = child.dis(traj);
                    elements.add(new Tuple2<>(child, dis));
                }
            }
            //logger.info("结束添加--el");
//            if (e._1.level() < g) {
//                for (EnlargedElement child : e._1.getChildren()) {
//                    double dis = child.dis(traj);
//                    elements.add(new Tuple2<>(child, dis));
//                }
//            }
        }
        System.out.println(currentThreshold + ", " + iterTimes + ", " + searched + ", " + searchedIS);
        return results;
    }

    public interface ResultProcess {
        void process(Result result);
    }

    public void query(List<Tuple2<Long, Long>> ranges, Table table, List<Filter> filter, ResultProcess process) throws Throwable {
        if (ranges.isEmpty()) {
            return;
        }
        final List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        for (Tuple2<Long, Long> a : ranges) {
            for (int i = 0; i < shard; i++) {
                byte[] startRow = new byte[9];
                byte[] endRow = new byte[9];
                startRow[0] = (byte) i;
                endRow[0] = (byte) i;
                ByteArrays.writeLong(a._1, startRow, 1);
                ByteArrays.writeLong(a._2, endRow, 1);
                rowRanges.add(new MultiRowRangeFilter.RowRange(startRow, true, endRow, false));
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
        //scan.setLimit()
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

    private boolean eeFilter(Tuple2<EnlargedElement, Double> e, double currentThreshold, Trajectory traj) {
        if (currentThreshold == Double.MAX_VALUE) {
            return false;
        }
        if (e._2 > currentThreshold) {
            return true;
        }
        //System.out.println("g:" +e._1.level() );
        //double maxResolution = traj.getXLength() + currentThreshold;
        //double minResolution = traj.getXLength() - currentThreshold;
        return false;
    }

    private boolean isFilter(Trajectory traj, Tuple2<IndexSpace, Double> is, double currentThreshold) {
        return is._2 > currentThreshold;
    }
}
