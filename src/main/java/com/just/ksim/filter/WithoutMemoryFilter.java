package com.just.ksim.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.just.ksim.similarity.Frechet;
import com.just.ksim.similarity.Hausdorff;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;
import utils.WKTUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static utils.Constants.GEOM;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 11:04
 * @modified by :
 **/
public class WithoutMemoryFilter extends FilterBase {
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


    public WithoutMemoryFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots, String mbrs, int func, boolean returnSim) {
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

    public WithoutMemoryFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots, String mbrs, boolean returnSim) {
        this(spoint, epoint, threshold, traj, pivots, mbrs, 0, returnSim);
    }

    public WithoutMemoryFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots) {
        this(spoint, epoint, threshold, traj, pivots, null, false);
    }

    public WithoutMemoryFilter(String spoint, String epoint, double threshold, String traj, List<Integer> pivots, String mbrs) {
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
        if (Bytes.toString(v.getQualifier()).equals(GEOM)) {
            Geometry geom = WKTUtils.read(Bytes.toString(v.getValue()));
            if (func == 0) {
                assert geom != null;
                double th = Frechet.calulateDistance(trajGeo, geom);
                this.filterRow = th > threshold;
                this.currentThreshold = BigDecimal.valueOf(th);
            } else if (func == 1) {
                assert geom != null;
                Tuple2<Boolean, Double> th = Hausdorff.calulateDistance(trajGeo, geom, threshold);
                this.filterRow = !th._1;
                this.currentThreshold = BigDecimal.valueOf(th._2);
            }
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell v) {
        //System.out.println(Bytes.toString(v.getQualifierArray()));
        //System.out.println(Bytes.toString(v.getQualifier()).equals(GEOM));
        //v.getv
        if (returnSim && !filterRow && Bytes.toString(v.getQualifier()).equals(GEOM) && null != this.currentThreshold) {
            //System.out.println("-------");
            return CellUtil.createCell(v.getRow(), v.getFamily(), v.getQualifier(),
                    System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(Bytes.toString(v.getValue()) + "-" + this.currentThreshold.toString()));
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
        return new WithoutMemoryFilter(proto.getSpoint(), proto.getEpoint(), proto.getThreshold(), proto.getTraj(), proto.getPivotsList(), proto.getMbrs(), proto.getFunc(), proto.getReturnSim());
    }
}
