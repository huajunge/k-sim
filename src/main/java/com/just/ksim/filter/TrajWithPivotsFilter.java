package com.just.ksim.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.just.ksim.similarity.Frechet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;
import utils.WKTUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

import static utils.Constants.*;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 11:04
 * @modified by :
 **/
public class TrajWithPivotsFilter extends FilterBase {
    private String spoint;
    private String epoint;
    private double threshold;
    private List<Integer> pivots;
    private String mbrs;
    private String traj;
    private boolean filterRow = false;
    private boolean returnSim;
    private Geometry spointGeo;
    private Geometry epointGeo;
    private Geometry trajGeo;
    private Geometry mbrGeo;
    private String currentGeom;
    private BigDecimal currentThreshold = null;
    private int size = 0;

    public TrajWithPivotsFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots, String mbrs, boolean returnSim) {
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
        this.spointGeo = WKTUtils.read(spoint);
        this.epointGeo = WKTUtils.read(epoint);
        this.returnSim = returnSim;
    }

    public TrajWithPivotsFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots) {
        this(spoint, epoint, threshold, traj, pivots, null, false);
    }

    public TrajWithPivotsFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots, String mbrs) {
        this(spoint, epoint, threshold, traj, pivots, mbrs, false);
    }

    @Override
    public void reset() {
        this.filterRow = false;
        this.currentGeom = null;
        this.currentThreshold = null;
        this.size = 0;
    }

    @Override
    public boolean filterRow() {
        return this.filterRow;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        //System.out.println("-----");
        //size++;
        if (!this.filterRow) {
            if (Bytes.toString(v.getQualifier()).equals(START_POINT)) {
                //System.out.println("1");
                Geometry geom = WKTUtils.read(Bytes.toString(v.getValue()));
                if (null != geom) {
                    if (geom.distance(this.spointGeo) > this.threshold) {
                        this.filterRow = true;
                    }
                }
            } else if (Bytes.toString(v.getQualifier()).equals(END_POINT)) {
                //System.out.println("2");
                Geometry geom = WKTUtils.read(Bytes.toString(v.getValue()));
                if (null != geom) {
                    if (geom.distance(this.epointGeo) > this.threshold) {
                        this.filterRow = true;
                    }
                }
            } else if (Bytes.toString(v.getQualifier()).equals(GEOM)) {
                //System.out.println("3");
                String[] p = Bytes.toString(v.getValue()).split("--");
                //Geometry geom = WKTUtils.read(p[0]);
                Geometry othterMbrGeo = WKTUtils.read(p[1]);
                if (null != this.mbrGeo) {
                    for (int i = 0; i < Objects.requireNonNull(othterMbrGeo).getNumGeometries(); i++) {
                        if (othterMbrGeo.getGeometryN(i).distance(this.mbrGeo) > threshold) {
                            this.filterRow = true;
                            break;
                        }
                    }
                    for (int i = 0; i < Objects.requireNonNull(this.mbrGeo).getNumGeometries() && !this.filterRow; i++) {
                        if (this.mbrGeo.getGeometryN(i).distance(othterMbrGeo) > threshold) {
                            this.filterRow = true;
                            break;
                        }
                    }
                }
                //Geometry geom = WKTUtils.read(p[0]);
                if (!filterRow) {
                    String[] indexes = p[2].split(",");
                    Geometry otherTrajGeo = WKTUtils.read(p[0]);
                    if (null != othterMbrGeo) {
                        //System.out.println("5");
                        for (String index : indexes) {
                            if (null != index && !index.equals("") && !index.equals("$s")) {
                                assert otherTrajGeo != null;
                                if (otherTrajGeo.getGeometryN(Integer.parseInt(index)).distance(this.mbrGeo) > threshold) {
                                    this.filterRow = true;
                                    break;
                                }
                            }
                        }
                        for (int i = 0; i < this.pivots.size() && !this.filterRow; i++) {
                            if (trajGeo.getGeometryN(pivots.get(i)).distance(othterMbrGeo) > threshold) {
                                this.filterRow = true;
                                break;
                            }
                        }
                    }
                    if (!this.filterRow) {
                        this.currentGeom = p[0];
                        //System.out.println("6");
                        assert trajGeo != null;
                        assert otherTrajGeo != null;
                        double th = Frechet.calulateDistance(trajGeo, otherTrajGeo);
                        this.filterRow = th > threshold;
                        this.currentThreshold = BigDecimal.valueOf(th);
                    }
                }
            }
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell v) {
        //System.out.println("size:" + size);
        //System.out.println(Bytes.toString(v.getQualifierArray()));
        //System.out.println(Bytes.toString(v.getQualifier()).equals(GEOM));
        //v.getv
        if (returnSim && !filterRow && Bytes.toString(v.getQualifier()).equals(GEOM) && null != this.currentThreshold) {
            //System.out.println("-------");
            return CellUtil.createCell(v.getRow(), v.getFamily(), v.getQualifier(),
                    System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(this.currentGeom + "-" + this.currentThreshold.toString()));
        }
//        else if (!returnSim && Bytes.toString(v.getQualifier()).equals(GEOM)) {
//            return CellUtil.createCell(v.getRow(), v.getFamily(), v.getQualifier(),
//                    System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(this.currentGeom));
//        }
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
        return new TrajWithPivotsFilter(proto.getSpoint(), proto.getEpoint(), proto.getThreshold(), proto.getTraj(), proto.getPivotsList(), proto.getMbrs(), proto.getReturnSim());
    }
}
