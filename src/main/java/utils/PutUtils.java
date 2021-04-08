package utils;

import com.just.ksim.entity.Trajectory;
import com.just.ksim.index.XZStarSFC;
import com.just.ksim.utils.ByteArrays;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

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
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), Bytes.toBytes(id));
        StringBuilder indexString = new StringBuilder();
        for (Integer integer : traj.getDPFeature().getIndexes()) {
            indexString.append(integer).append(",");
        }
        String geom = String.format("%s--%s--$s",traj.toText(), traj.getDPFeature().getMBRs().toText(),indexString.toString());
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), Bytes.toBytes(traj.getGeometryN(0).toText()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), Bytes.toBytes(traj.getGeometryN(traj.getNumGeometries() - 1).toText()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM), Bytes.toBytes(geom));
        return put;
    }

    public Put getPut2(Trajectory traj, Short shard) {
        String id = traj.getId();
        long index = sfc.index(traj.getMultiPoint(), false);
        short s = (short) (index % shard);
        byte[] bytes = new byte[9 + id.length()];
        bytes[0] = (byte) s;
        ByteArrays.writeLong(index, bytes, 1);
        System.arraycopy(Bytes.toBytes(id), 0, bytes, 9, id.length());
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), Bytes.toBytes(id));
        StringBuilder indexString = new StringBuilder();
        for (Integer integer : traj.getDPFeature().getIndexes()) {
            indexString.append(integer).append(",");
        }
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT), Bytes.toBytes(traj.getDPFeature().getMBRs().toText() + "--" + indexString.toString()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), Bytes.toBytes(traj.getGeometryN(0).toText()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), Bytes.toBytes(traj.getGeometryN(traj.getNumGeometries() - 1).toText()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM), Bytes.toBytes(traj.toText()));
        return put;
    }
}
