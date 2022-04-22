package just.urbancomputing.endpoint;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import coprocessor.generated.KNNServer;
import just.urbancomputing.similarity.Frechet;
import just.urbancomputing.similarity.Hausdorff;
import just.urbancomputing.utils.ByteArrays;
import just.urbancomputing.utils.WKTUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static just.urbancomputing.utils.Constants.*;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-09-14 11:50
 * @modified by :
 **/
public class KNNCoprocessor extends KNNServer.KnnService implements Coprocessor, CoprocessorService {
    RegionCoprocessorEnvironment env;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void getTopK(RpcController controller, KNNServer.KnnRequest request, RpcCallback<KNNServer.KnnResponse> done) {
        //this.env.getRegion().getScanner()
        final List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        for (KNNServer.Range a : request.getRangesList()) {
            for (int i = 0; i < 4; i++) {
                byte[] startRow = new byte[9];
                byte[] endRow = new byte[9];
                startRow[0] = (byte) i;
                endRow[0] = (byte) i;
                ByteArrays.writeLong(a.getStart(), startRow, 1);
                ByteArrays.writeLong(a.getEnd(), endRow, 1);
                rowRanges.add(new MultiRowRangeFilter.RowRange(startRow, true, endRow, true));
            }
        }
        Scan scan = new Scan();
        scan.setFilter(new MultiRowRangeFilter(rowRanges));
        InternalScanner resultScanner = null;
        MinMaxPriorityQueue<Tuple2<KNNServer.Traj, Double>> trajs =
                MinMaxPriorityQueue.orderedBy((Comparator<Tuple2<KNNServer.Traj, Double>>) (e1, e2) -> Double.compare(e1._2, e2._2))
                        .maximumSize(request.getK()).create();
        try {
            resultScanner = this.env.getRegion().getScanner(scan);

            //assert resultScanner != null;
            List<Cell> results = new ArrayList<>();
            boolean hasMore = false;
            int size = 0;
            double currentThreshold = request.getThreshold();
            Geometry trajGeo = WKTUtils.read(request.getTraj());

            Geometry spointGeo = trajGeo.getGeometryN(0);
            Geometry epointGeo = trajGeo.getGeometryN(trajGeo.getNumGeometries() - 1);
            Geometry mbrGeo = WKTUtils.read(request.getMbrs());
            Geometry othterMbrGeo = null;
            String[] indexes = new String[0];
            List<Integer> pivots = request.getPivotsList();
            do {
                try {
                    hasMore = resultScanner.next(results);
                } catch (IOException e) {
                    e.printStackTrace();
                    ResponseConverter.setControllerException(controller, e);
                }
                boolean filterRow = false;
                Geometry otherTrajGeo;
                for (Cell v : results) {
                    if (Double.MAX_VALUE != currentThreshold) {
                        if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(START_POINT) && request.getFunc() == 0) {
                            //System.out.println("1");
                            Geometry geom = WKTUtils.read(Bytes.toString(CellUtil.cloneValue(v)));
                            if (null != geom) {
                                if (geom.distance(spointGeo) > currentThreshold) {
                                    break;
                                }
                            }
                        } else if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(END_POINT) && request.getFunc() == 0) {
                            //System.out.println("2");
                            Geometry geom = WKTUtils.read(Bytes.toString(CellUtil.cloneValue(v)));
                            if (null != geom) {
                                if (geom.distance(epointGeo) > currentThreshold) {
                                    break;
                                }
                            }
                        } else if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(PIVOT)) {
                            //System.out.println("3");
                            long time = System.currentTimeMillis();
                            String[] p = Bytes.toString(CellUtil.cloneValue(v)).split("--");
                            Geometry geom = WKTUtils.read(p[0]);
                            othterMbrGeo = geom;
                            if (null != mbrGeo) {
                                for (int i = 0; i < Objects.requireNonNull(geom).getNumGeometries(); i++) {
                                    if (geom.getGeometryN(i).distance(mbrGeo) > currentThreshold) {
                                        filterRow = true;
                                        break;
                                    }
                                }
                                for (int i = 0; i < Objects.requireNonNull(mbrGeo).getNumGeometries() && !filterRow; i++) {
                                    if (mbrGeo.getGeometryN(i).distance(geom) > currentThreshold) {
                                        filterRow = true;
                                        break;
                                    }
                                }
                                if (filterRow) {
                                    break;
                                }
                            }
                            //Geometry geom = WKTUtils.read(p[0]);
                            indexes = p[1].split(",");
                            //System.out.println("mbr time:" + (System.currentTimeMillis() - time));
                        } else if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(GEOM)) {
                            Geometry geom = WKTUtils.read(Bytes.toString(CellUtil.cloneValue(v)));
                            //System.out.println("4");
                            otherTrajGeo = geom;
                            if (null != otherTrajGeo) {
                                //Geometry trajGeo = WKTUtils.read(this.traj);
                                //long time = System.currentTimeMillis();
                                if (null != indexes && null != othterMbrGeo) {
                                    //System.out.println("5");
                                    //time = System.currentTimeMillis();
                                    for (String index : indexes) {
                                        if (null != index && !index.equals("") && !index.equals("$s")) {
                                            if (otherTrajGeo.getGeometryN(Integer.parseInt(index)).distance(mbrGeo) > currentThreshold) {
                                                filterRow = true;
                                                break;
                                            }
                                        }
                                    }
                                    for (int i = 0; i < pivots.size() && !filterRow; i++) {
                                        if (trajGeo.getGeometryN(pivots.get(i)).distance(othterMbrGeo) > currentThreshold) {
                                            filterRow = true;
                                            break;
                                        }
                                    }
                                    //System.out.println("points time:" + (System.currentTimeMillis() - time));
                                }
                                if (request.getFunc() == 0) {
                                    double th = Frechet.calulateDistance(trajGeo, otherTrajGeo);
                                    if (th < currentThreshold) {
                                        trajs.add(new Tuple2<>(KNNServer.Traj.newBuilder().setTrajId("id").setTrajGeom(otherTrajGeo.toText()).setSim((float) th).build(), th));
                                    }

                                } else if (request.getFunc() == 1) {
                                    Tuple2<Boolean, Double> th = Hausdorff.calulateDistance(trajGeo, otherTrajGeo, currentThreshold);
                                    if (th._1 && th._2 < currentThreshold) {
                                        trajs.add(new Tuple2<>(KNNServer.Traj.newBuilder().setTrajId("id").setTrajGeom(otherTrajGeo.toText()).setSim(th._2.floatValue()).build(), th._2));
                                        size++;
                                        if (size >= request.getK()) {
                                            currentThreshold = trajs.peekLast()._2;
                                        }
                                    }
                                }
                            }
                        }
                    } else if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(GEOM)) {
                        //System.out.println("-------");
                        String geomStr = Bytes.toString(CellUtil.cloneValue(v));
                        Geometry geom = WKTUtils.read(geomStr);
                        if (null != geom) {
                            assert trajGeo != null;
                            BigDecimal threshold = null;
                            if (request.getFunc() == 0) {
                                threshold = BigDecimal.valueOf(Frechet.calulateDistance(trajGeo, geom));
                            } else if (request.getFunc() == 1) {
                                threshold = BigDecimal.valueOf(Hausdorff.calulateDistance(trajGeo, geom));
                            }
                            //BigDecimal d1 = new BigDecimal(threshold);
                            if (size < request.getK()) {
                                assert threshold != null;
                                trajs.add(new Tuple2<>(KNNServer.Traj.newBuilder().setTrajId("id").setTrajGeom(geomStr).setSim((float) threshold.doubleValue()).build(), threshold.doubleValue()));
                                size++;
                                if (size >= request.getK()) {
                                    currentThreshold = trajs.peekLast()._2;
                                }
                            }
                        }
                    }
                }
                results.clear();

            } while (hasMore);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (resultScanner != null) {
                try {
                    resultScanner.close();
                } catch (IOException ignored) {
                }
            }
        }
        KNNServer.KnnResponse response = KNNServer.KnnResponse.newBuilder().addAllTrajs(trajs.stream().map(v -> v._1).collect(Collectors.toList())).build();
        done.run(response);
        //List<KNNServer.Traj> resultTraj = new ArrayList<>(trajs.size());
    }
}
