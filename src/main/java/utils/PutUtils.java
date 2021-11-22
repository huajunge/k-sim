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

//    public Put getPut(Trajectory traj, Short shard) {
//        String id = traj.getId();
//        long index = sfc.index(traj.getMultiPoint(), false);
//        short s = (short) (index % shard);
//        byte[] bytes = new byte[9 + id.length()];
//        bytes[0] = (byte) s;
//        ByteArrays.writeLong(index, bytes, 1);
//        System.arraycopy(Bytes.toBytes(id), 0, bytes, 9, id.length());
//        Put put = new Put(bytes);
//        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), Bytes.toBytes(id));
//        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), Bytes.toBytes(traj.getGeometryN(0).toText()));
//        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), Bytes.toBytes(traj.getGeometryN(traj.getNumGeometries() - 1).toText()));
//        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM), Bytes.toBytes(traj.getMultiPoint().toText()));
//        return put;
//    }

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
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT), Bytes.toBytes(traj.getDPFeature().getMBRs().toText() + "--" + indexString.toString()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), Bytes.toBytes(traj.getGeometryN(0).toText()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), Bytes.toBytes(traj.getGeometryN(traj.getNumGeometries() - 1).toText()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM), Bytes.toBytes(traj.toText()));
        return put;
    }

    public Put getQuadTreePut(Trajectory traj, Short shard) {
        String id = traj.getId();
        long index = sfc.indexQuadTree(traj.getMultiPoint(), false);
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

    public Put getPutString(Trajectory traj, Short shard) {
        String id = traj.getId();
        String index = sfc.indexLength(traj.getMultiPoint(), false);
        int length = Bytes.toBytes(index).length;
        short s = (short) (sfc.index(traj.getMultiPoint(), false) % shard);
        byte[] bytes = new byte[1 + length + id.length()];
        bytes[0] = (byte) s;

        //ByteArrays.writeLong(index, bytes, 1);
        System.arraycopy(Bytes.toBytes(index), 0, bytes, 1, length);
        System.arraycopy(Bytes.toBytes(id), 0, bytes, length + 1, id.length());
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
