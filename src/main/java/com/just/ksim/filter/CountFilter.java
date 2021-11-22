package com.just.ksim.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;
import utils.WKTUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static utils.Constants.*;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 11:04
 * @modified by :
 **/
public class CountFilter extends FilterBase {
    private String spoint;
    private String epoint;
    private double threshold;
    private List<String> pivots;
    private String traj;
    private boolean filterRow = false;
    private boolean checkedAllPoint = false;
    private boolean returnSim;
    private Geometry spointGeo;
    private Geometry epointGeo;
    private BigDecimal currentThreshold = null;

    public CountFilter(String spoint, String epoint, double threshold, String traj, List<String> pivots, boolean returnSim) {
        this.spoint = spoint;
        this.epoint = epoint;
        this.threshold = threshold;
        this.traj = traj;
        this.pivots = pivots;
        this.spointGeo = WKTUtils.read(spoint);
        this.epointGeo = WKTUtils.read(epoint);
        this.returnSim = returnSim;
    }

    public CountFilter(String spoint, String epoint, double threshold, String traj, List<String> pivots) {
        this(spoint, epoint, threshold, traj, pivots, false);
    }

    @Override
    public void reset() {
        this.filterRow = false;
        this.checkedAllPoint = false;
    }

    @Override
    public boolean filterRow() {
        return this.filterRow;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (!this.checkedAllPoint && !this.filterRow) {
            if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(START_POINT)) {
                Geometry geom = WKTUtils.read(Bytes.toString(CellUtil.cloneValue(v)));
                if (null != geom) {
                    if (geom.distance(this.spointGeo) > this.threshold) {
                        this.filterRow = true;
                    }
                }
            } else if (Bytes.toString(CellUtil.cloneQualifier(v)).equals(END_POINT)) {
                Geometry geom = WKTUtils.read(Bytes.toString(CellUtil.cloneValue(v)));
                if (null != geom) {
                    if (geom.distance(this.epointGeo) > this.threshold) {
                        this.filterRow = true;
                    }
                }
            }
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public byte[] toByteArray() {
        filters.generated.CountFilter.CountFilters.Builder builder =
                filters.generated.CountFilter.CountFilters.newBuilder();
        builder.setSpoint(this.spoint);
        builder.setEpoint(this.epoint);
        builder.setThreshold(this.threshold);
        builder.setTraj(this.traj);
        builder.setReturnSim(this.returnSim);
        if (null != this.pivots) {
            builder.addAllPivots(this.pivots);
        }
//        if (value != null) {
//            builder.setValue(ByteStringer.wrap(value)); // co CustomFilter-6-Write Writes the given value out so it can be sent to the servers.
//        }
        return builder.build().toByteArray();
    }

    public static Filter parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        filters.generated.CountFilter.CountFilters proto;
        try {
            proto = filters.generated.CountFilter.CountFilters.parseFrom(pbBytes); // co CustomFilter-7-Read Used by the servers to establish the filter instance with the correct values.
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new CountFilter(proto.getSpoint(), proto.getEpoint(), proto.getThreshold(), proto.getTraj(), proto.getPivotsList(), proto.getReturnSim());
    }
}
