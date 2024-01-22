package com.just.ksim.urbancomputing.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.just.ksim.urbancomputing.utils.Constants;
import com.just.ksim.urbancomputing.utils.WKTUtils;
import com.just.ksim.urbancomputing.similarity.DTW;
import com.just.ksim.urbancomputing.similarity.Frechet;
import com.just.ksim.urbancomputing.similarity.Hausdorff;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;


/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 11:04
 * @modified by :
 **/
public class PivotsFilter extends FilterBase {
    private String spoint;
    private String epoint;
    private double threshold;
    private List<Integer> pivots;
    private String mbrs;
    private String traj;
    private int func;
    private boolean filterRow = false;
    private boolean returnSim;
    private Geometry spointGeo;
    private Geometry epointGeo;
    private Geometry trajGeo;
    private Geometry otherTrajGeo;
    private Geometry mbrGeo;
    private Geometry othterMbrGeo;
    private String[] indexes;
    private BigDecimal currentThreshold = null;


    public PivotsFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots, String mbrs, int func, boolean returnSim) {
        this.spoint = spoint;
        this.epoint = epoint;
        this.threshold = threshold;
        this.traj = traj;
        this.trajGeo = WKTUtils.read(traj);
        if (null != mbrs) {
            this.mbrGeo = WKTUtils.read(mbrs);
        }
        this.pivots = pivots;
        this.mbrs = mbrs;
        this.func = func;
        this.spointGeo = WKTUtils.read(spoint);
        this.epointGeo = WKTUtils.read(epoint);
        this.returnSim = returnSim;
    }

    public PivotsFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots, String mbrs, boolean returnSim) {
        this(spoint, epoint, threshold, traj, pivots, mbrs, 0, returnSim);
    }

    public PivotsFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots) {
        this(spoint, epoint, threshold, traj, pivots, null, false);
    }

    public PivotsFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots, String mbrs) {
        this(spoint, epoint, threshold, traj, pivots, mbrs, false);
    }

    @Override
    public void reset() {
        this.filterRow = false;
        this.othterMbrGeo = null;
        this.otherTrajGeo = null;
        this.indexes = null;
    }

    @Override
    public boolean filterRow() {
        return this.filterRow;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (!this.filterRow) {
            if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(Constants.START_POINT) && func == 0) {
                //System.out.println("1");
                Geometry geom = WKTUtils.read(Bytes.toString(CellUtil.cloneValue(v)));
                if (null != geom) {
                    if (geom.distance(this.spointGeo) > this.threshold) {
                        this.filterRow = true;
                    }
                }
            } else if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(Constants.END_POINT) && func == 0) {
                //System.out.println("2");
                Geometry geom = WKTUtils.read(Bytes.toString(CellUtil.cloneValue(v)));
                if (null != geom) {
                    if (geom.distance(this.epointGeo) > this.threshold) {
                        this.filterRow = true;
                    }
                }
            } else if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(Constants.PIVOT)) {
                //System.out.println("3");
                long time = System.currentTimeMillis();
                String[] p = Bytes.toString(CellUtil.cloneValue(v)).split("--");
                Geometry geom = WKTUtils.read(p[0]);
                this.othterMbrGeo = geom;
                if (null != this.mbrGeo) {
                    for (int i = 0; i < Objects.requireNonNull(geom).getNumGeometries(); i++) {
                        if (geom.getGeometryN(i).distance(this.mbrGeo) > threshold) {
                            this.filterRow = true;
                            break;
                        }
                    }
                    for (int i = 0; i < Objects.requireNonNull(this.mbrGeo).getNumGeometries() && !this.filterRow; i++) {
                        if (this.mbrGeo.getGeometryN(i).distance(geom) > threshold) {
                            this.filterRow = true;
                            break;
                        }
                    }
                }
                //Geometry geom = WKTUtils.read(p[0]);
                indexes = p[1].split(",");
                //System.out.println("mbr time:" + (System.currentTimeMillis() - time));
            } else if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(Constants.GEOM)) {
                Geometry geom = WKTUtils.read(Bytes.toString(CellUtil.cloneValue(v)));
                //System.out.println("4");
                otherTrajGeo = geom;
                if (null != otherTrajGeo && !this.filterRow) {
                    //Geometry trajGeo = WKTUtils.read(this.traj);
                    //long time = System.currentTimeMillis();
                    if (null != this.indexes && null != this.othterMbrGeo) {
                        //System.out.println("5");
                        //time = System.currentTimeMillis();
                        for (String index : this.indexes) {
                            if (null != index && !index.equals("") && !index.equals("$s")) {
                                if (otherTrajGeo.getGeometryN(Integer.parseInt(index)).distance(this.mbrGeo) > threshold) {
                                    this.filterRow = true;
                                    break;
                                }
                            }
                        }
                        for (int i = 0; i < this.pivots.size() && !this.filterRow; i++) {
                            if (trajGeo.getGeometryN(pivots.get(i)).distance(this.othterMbrGeo) > threshold) {
                                this.filterRow = true;
                                break;
                            }
                        }
                        //System.out.println("points time:" + (System.currentTimeMillis() - time));
                    }
                    if (!this.filterRow) {
                        //System.out.println("6");
                        //time = System.currentTimeMillis();
                        assert trajGeo != null;
                        if (func == 0) {
                            double th = Frechet.calulateDistance(trajGeo, otherTrajGeo);
                            this.filterRow = th > threshold;
                            this.currentThreshold = BigDecimal.valueOf(th);
                        } else if (func == 1) {
                            Tuple2<Boolean, Double> th = Hausdorff.calulateDistance(trajGeo, otherTrajGeo, threshold);
                            this.filterRow = !th._1;
                            this.currentThreshold = BigDecimal.valueOf(th._2);
                        } else if (func == 2) {
                            double th = DTW.calulateDistance(trajGeo, otherTrajGeo);
                            this.filterRow = th > threshold;
                            this.currentThreshold = BigDecimal.valueOf(th);
                        }
                        //System.out.println("calulateDistance time:" + (System.currentTimeMillis() - time));
                    }
                }
            }
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell v) {
        //System.out.println(Bytes.toString(CellUtil.cloneQualifierArray()));
        //System.out.println(Bytes.toString(CellUtil.cloneQualifier()).equals(GEOM));
        //CellUtil.clonev
        if (returnSim && !filterRow && Bytes.toString(CellUtil.cloneQualifier(v)).equals(Constants.GEOM) && null != this.currentThreshold) {
            //System.out.println("-------");
            return CellUtil.createCell(CellUtil.cloneRow(v), CellUtil.cloneFamily(v), CellUtil.cloneQualifier(v),
                    System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(Bytes.toString(CellUtil.cloneValue(v)) + "-" + this.currentThreshold.toString()));
        }
        return v;
    }


    @Override
    public byte[] toByteArray() {
        filters.generated.PivotPointFilter.PivotsFilter2.Builder builder =
                filters.generated.PivotPointFilter.PivotsFilter2.newBuilder();
        builder.setSpoint(this.spoint);
        builder.setEpoint(this.epoint);
        builder.setThreshold(this.threshold);
        builder.setTraj(this.traj);
        builder.setFunc(this.func);
        builder.setReturnSim(this.returnSim);
        if (null != this.pivots) {
            builder.addAllPivots(this.pivots);
        }
        if (null != this.mbrs) {
            builder.setMbrs(this.mbrs);
        }
//        if (value != null) {
//            builder.setValue(ByteStringer.wrap(value)); // co CustomFilter-6-Write Writes the given value out so it can be sent to the servers.
//        }
        return builder.build().toByteArray();
    }

    public static Filter parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        filters.generated.PivotPointFilter.PivotsFilter2 proto;
        try {
            proto = filters.generated.PivotPointFilter.PivotsFilter2.parseFrom(pbBytes); // co CustomFilter-7-Read Used by the servers to establish the filter instance with the correct values.
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new PivotsFilter(proto.getSpoint(), proto.getEpoint(), proto.getThreshold(), proto.getTraj(), proto.getPivotsList(), proto.getMbrs(), proto.getFunc(), proto.getReturnSim());
    }
}
