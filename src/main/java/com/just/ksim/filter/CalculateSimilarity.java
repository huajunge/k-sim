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
import util.WKTUtils;

import java.math.BigDecimal;

import static util.Constants.GEOM;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 18:23
 * @modified by :
 **/
public class CalculateSimilarity extends FilterBase {
    private String traj;

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
        if (Bytes.toString(v.getQualifier()).equals(GEOM)) {
            //System.out.println("-------");
            Geometry geom = WKTUtils.read(Bytes.toString(v.getValue()));
            if (null != geom) {
                Geometry trajGeo = WKTUtils.read(this.traj);
                assert trajGeo != null;
                BigDecimal threshold = BigDecimal.valueOf(Frechet.calulateDistance(trajGeo, geom));
                //BigDecimal d1 = new BigDecimal(threshold);
                return CellUtil.createCell(v.getRow(), v.getFamily(), v.getQualifier(),
                        System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(Bytes.toString(v.getValue()) + "-" + threshold.toString()));
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
