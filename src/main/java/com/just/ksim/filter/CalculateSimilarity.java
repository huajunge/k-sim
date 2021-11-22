package com.just.ksim.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.just.ksim.similarity.Frechet;
import com.just.ksim.similarity.Hausdorff;
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

import java.math.BigDecimal;

import static utils.Constants.GEOM;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 18:23
 * @modified by :
 **/
public class CalculateSimilarity extends FilterBase {
    private String traj;
    private int func = 0;

    public CalculateSimilarity(String traj, int func) {
        this.traj = traj;
        this.func = func;
    }

    public CalculateSimilarity(String traj) {
        this.traj = traj;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell v) {
        //System.out.println(Bytes.toString(v.getQualifierArray()));
        //System.out.println(Bytes.toString(v.getQualifier()).equals(GEOM));
        //v.getv
        if (Bytes.toString(v.getQualifierArray(), v.getQualifierOffset(), v.getQualifierLength()).equals(GEOM)) {
            //Bytes.toString(v.getQualifierArray(), v.getQualifierOffset(), v.getQualifierLength());
            //System.out.println("-------");
            Geometry geom = WKTUtils.read(Bytes.toString(v.getValueArray(), v.getValueOffset(), v.getValueLength()));
            if (null != geom) {
                Geometry trajGeo = WKTUtils.read(this.traj);
                assert trajGeo != null;
                BigDecimal threshold = null;
                if (func == 0) {
                    threshold = BigDecimal.valueOf(Frechet.calulateDistance(trajGeo, geom));
                } else if (func == 1) {
                    threshold = BigDecimal.valueOf(Hausdorff.calulateDistance(trajGeo, geom));
                }
                //BigDecimal d1 = new BigDecimal(threshold);
                return CellUtil.createCell(CellUtil.cloneRow(v), CellUtil.cloneFamily(v), CellUtil.cloneQualifier(v),
                        System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(Bytes.toString(CellUtil.cloneValue(v)) + "-" + threshold.toString()));
            }
        }
        return v;
    }

    @Override
    public byte[] toByteArray() {
        Similarity.SimilarityFilter.Builder builder =
                Similarity.SimilarityFilter.newBuilder();
        builder.setTraj(this.traj);
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
        return new CalculateSimilarity(proto.getTraj());
    }
}
