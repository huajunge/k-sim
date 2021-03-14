package com.just.ksim.similarity;

import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author : hehuajun3
 * @description : Hausdorff
 * @date : Created in 2021-03-13 14:08
 * @modified by :
 **/
public class Hausdorff {
    public static double calulateDistance(Geometry search, Geometry queried) {
        int n = search.getNumGeometries();
        int m = queried.getNumGeometries();
        double dis;
        double maxDis = 0.0;
        for (int i = 0; i < n; i++) {
            double minD = Double.MAX_VALUE;
            for (int j = 0; j < m; j++) {
                dis = search.getGeometryN(i).distance(queried.getGeometryN(j));
                if (dis < minD) {
                    minD = dis;
                }
            }
            if (maxDis < minD) {
                maxDis = minD;
            }
        }

        for (int i = 0; i < m; i++) {
            double minD = Double.MAX_VALUE;
            for (int j = 0; j < n; j++) {
                dis = search.getGeometryN(j).distance(queried.getGeometryN(i));
                if (dis < minD) {
                    minD = dis;
                }
            }
            if (maxDis < minD) {
                maxDis = minD;
            }
        }
        return maxDis;
    }


    public static Tuple2<Boolean, Double> calulateDistance(Geometry search, Geometry queried, Double filterThreshold) {
        int n = search.getNumGeometries();
        int m = queried.getNumGeometries();
        double dis;
        double maxDis = 0.0;
        boolean filter = (null != filterThreshold);
        for (int i = 0; i < n; i++) {
            double minD = Double.MAX_VALUE;
            for (int j = 0; j < m; j++) {
                dis = search.getGeometryN(i).distance(queried.getGeometryN(j));
                if (dis < minD) {
                    minD = dis;
                }
            }
            if (maxDis < minD) {
                maxDis = minD;
            }
            if (filter && maxDis > filterThreshold) {
                return new Tuple2<>(false, maxDis);
            }
        }

        for (int i = 0; i < m; i++) {
            double minD = Double.MAX_VALUE;
            for (int j = 0; j < n; j++) {
                dis = search.getGeometryN(j).distance(queried.getGeometryN(i));
                if (dis < minD) {
                    minD = dis;
                }
            }
            if (maxDis < minD) {
                maxDis = minD;
            }
            if (filter && maxDis > filterThreshold) {
                return new Tuple2<>(false, maxDis);
            }
        }
        return new Tuple2<>(true, maxDis);
    }
}
