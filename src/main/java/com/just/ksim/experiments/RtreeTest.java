package com.just.ksim.experiments;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.just.ksim.entity.Trajectory;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.MultiPoint;
import rx.Observable;
import utils.WKTUtils;

import java.io.*;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-06-19 14:13
 * @modified by :
 **/
public class RtreeTest {
    public static void main(String[] args) throws IOException {
        String filePath = "D:\\工作文档\\data\\T-drive\\release\\tdrive\\1";
        String queryPath = "D:\\工作文档\\data\\T-drive\\release\\tdrive_q";
//        Path path = new Path(filePath);
//        FileSystem fs = path.getFileSystem(new Configuration());
        //BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        //RTree<Trajectory, Rectangle> rTree = RTree.star().create();
        RTree<Rectangle, Rectangle> rTree = RTree.star().create();
        File file = new File(filePath);
        long time = System.currentTimeMillis();
        long readTime = 0;
        for (File listFile : Objects.requireNonNull(file.listFiles())) {
            long t1 = System.currentTimeMillis();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(listFile)));
            Stream<Trajectory> trajs = reader.lines().map(traj -> {
                String[] t = traj.split("-");
                return new Trajectory(t[0], (MultiPoint) WKTUtils.read(t[1]));
            });
            readTime += System.currentTimeMillis() - t1;
            for (Trajectory trajectory : trajs.collect(Collectors.toList())) {
                Envelope bbox = trajectory.getMultiPoint().getEnvelopeInternal();
                trajectory.getDPFeature();
                rTree = rTree.add(Geometries.rectangleGeographic(bbox.getMinX(),
                        bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()), Geometries.rectangleGeographic(bbox.getMinX(),
                        bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()));
            }
        }
        System.out.println(System.currentTimeMillis() - time - readTime);
        File queryFile = new File(queryPath);
        for (File listFile : Objects.requireNonNull(queryFile.listFiles())) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(listFile)));
            Stream<Trajectory> trajs = reader.lines().map(traj -> {
                String[] t = traj.split("-");
                return new Trajectory(t[0], (MultiPoint) WKTUtils.read(t[1]));
            });
            for (Trajectory trajectory : trajs.collect(Collectors.toList())) {
                Envelope bbox = trajectory.getMultiPoint().getEnvelopeInternal();
                Observable<Entry<Rectangle, Rectangle>> result = rTree.nearest(Geometries.rectangleGeographic(bbox.getMinX(),
                        bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()), 0.002, Integer.MAX_VALUE);
                Observable<Entry<Rectangle, Rectangle>> result2 = rTree.nearest(
                        Geometries.rectangleGeographic(bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()), 0.002, Integer.MAX_VALUE);
                System.out.println("size:" + result.count().toBlocking().first());
            }
        }
        //System.out.println(RamUsageEstimator.humanSizeOf(rTree));
    }
}
