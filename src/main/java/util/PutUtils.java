package util;

import com.just.ksim.entity.Trajectory;
import com.just.ksim.index.XZStarSFC;
import com.just.ksim.utils.ByteArrays;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

import static util.Constants.*;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-15 14:35
 * @modified by :
 **/
public class PutUtils implements Serializable {
    private Short g;
    private XZStarSFC sfc;

    public PutUtils(Short g) {
        this(g, 1);
    }

    public PutUtils(Short g, int beta) {
        this.g = g;
        this.sfc = XZStarSFC.apply(g, beta);
    }

    public Put getPut(Trajectory traj, Short shard) {
        String id = traj.getId();
        long index = sfc.index(traj.getMultiPoint(), false);
        short s = (short) (index % shard);
        byte[] bytes = new byte[9 + id.length()];
        bytes[0] = (byte) s;
        ByteArrays.writeLong(index, bytes, 1);
        System.arraycopy(Bytes.toBytes(id), 0, bytes, 9, id.length());
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID), Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM), Bytes.toBytes(traj.toText()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(START_POINT), Bytes.toBytes(traj.getGeometryN(0).toText()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(END_POINT), Bytes.toBytes(traj.getGeometryN(traj.getNumGeometries() - 1).toText()));
        return put;
    }

}
