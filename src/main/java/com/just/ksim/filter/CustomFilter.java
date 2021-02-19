package com.just.ksim.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;

import java.io.IOException;

/**
 * @author : hehuajun3
 * @description : customFilter
 * @date : Created in 2021-01-30 15:16
 * @modified by :
 **/
public class CustomFilter extends FilterBase {

    private byte[] value = null;
    private boolean filterRow = true;

    public CustomFilter() {
    }

    public CustomFilter(byte[] value) {
        this.value = value;
    }

    @Override
    public void reset() {
        this.filterRow = true;
    }

    @Override
    public boolean filterRow() {
        return this.filterRow;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        if (CellUtil.matchingValue(v, value)) {
            filterRow = false;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        filters.generated.FilterProtos.CustomFilter.Builder builder =
                filters.generated.FilterProtos.CustomFilter.newBuilder();
        if (value != null) {
            builder.setValue(ByteStringer.wrap(value)); // co CustomFilter-6-Write Writes the given value out so it can be sent to the servers.
        }
        return builder.build().toByteArray();
    }

    public static Filter parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        filters.generated.FilterProtos.CustomFilter proto;
        try {
            proto = filters.generated.FilterProtos.CustomFilter.parseFrom(pbBytes); // co CustomFilter-7-Read Used by the servers to establish the filter instance with the correct values.
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new CustomFilter(proto.getValue().toByteArray());
    }
}
