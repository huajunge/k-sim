package com.just.ksim.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.just.ksim.similarity.Frechet;
import filters.generated.Similarity;
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

import static utils.Constants.GEOM;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 18:23
 * @modified by :
 **/
public class SimilarityFilter extends FilterBase {
    private String traj;
    private double threshold;
    private boolean filterRow = false;
    private Geometry trajGeo;
    private BigDecimal currentThreshold = null;
    private String tmpGeo = null;
    private boolean returnSim;

    public SimilarityFilter(String traj, double threshold, boolean returnSim) {
        this.traj = traj;
        this.trajGeo = WKTUtils.read(traj);
        this.threshold = threshold;
        this.returnSim = returnSim;
    }

    public SimilarityFilter(String traj, double threshold) {
        this(traj, threshold, false);
    }

    @Override
    public void reset() {
        this.filterRow = false;
        this.trajGeo = null;
        this.tmpGeo = null;
    }

    @Override
    public boolean filterRow() throws IOException {
        return this.filterRow;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        if (Bytes.toString(v.getQualifier()).equals(GEOM)) {
            System.out.println("6");
            this.tmpGeo = Bytes.toString(v.getValue());
            Geometry otherTrajGeo = WKTUtils.read(tmpGeo);
            assert trajGeo != null;
            assert otherTrajGeo != null;
            double th = Frechet.calulateDistance(trajGeo, otherTrajGeo);
            this.filterRow = th > this.threshold;
            this.currentThreshold = BigDecimal.valueOf(th);
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell v) {
        //System.out.println(Bytes.toString(v.getQualifierArray()));
        //System.out.println(Bytes.toString(v.getQualifier()).equals(GEOM));
        //v.getv
        if (Bytes.toString(v.getQualifier()).equals(GEOM) && !this.filterRow && this.returnSim) {
            //System.out.println("-------");
            if (null != this.tmpGeo && null != this.currentThreshold) {
                //BigDecimal d1 = new BigDecimal(threshold);
                return CellUtil.createCell(v.getRow(), v.getFamily(), v.getQualifier(),
                        System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(Bytes.toString(v.getValue()) + "-" + currentThreshold.toString()));
            }
        }
        return v;
    }

    @Override
    public byte[] toByteArray() {
        Similarity.SimilarityFilter.Builder builder =
                Similarity.SimilarityFilter.newBuilder();
        builder.setTraj(this.traj);
        builder.setThreshold(this.threshold);
        builder.setReturnSim(this.returnSim);
//        if (value != null) {
//            builder.setValue(ByteStringer.wrap(value)); // co CustomFilter-6-Write Writes the given value out so it can be sent to the servers.
//        }
        return builder.build().toByteArray();
    }

    public static Filter parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        Similarity.SimilarityFilter proto;
        try {
            proto = Similarity.SimilarityFilter.parseFrom(pbBytes); // co CustomFilter-7-Read Used by the servers to establish the filter instance with the correct values.
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new SimilarityFilter(proto.getTraj(), proto.getThreshold(), proto.getReturnSim());
    }
}
