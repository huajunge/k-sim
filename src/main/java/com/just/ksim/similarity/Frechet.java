package com.just.ksim.similarity;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import util.NumberUtil;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 12:06
 * @modified by :
 **/
public class Frechet {
    public static double calulateDistance(Geometry search, Geometry queried) {
        int n = search.getNumGeometries();
        int m = queried.getNumGeometries();
        double[][] minDis = new double[n][m];
        minDis[0][0] = search.getGeometryN(0).distance(queried.getGeometryN(0));
        for (int i = 1; i < n; i++) {
            minDis[i][0] = Double.max(minDis[i][0], search.getGeometryN(i).distance(queried.getGeometryN(0)));
        }

        for (int j = 1; j < m; j++) {
            minDis[0][j] = Double.max(minDis[0][j], search.getGeometryN(0).distance(queried.getGeometryN(j)));
        }

        for (int i = 1; i < n; i++) {
            for (int j = 1; j < m; j++) {
                minDis[i][j] = Double.max(NumberUtil.min(minDis[i - 1][j], minDis[i][j - 1], minDis[i - 1][j - 1]),
                        search.getGeometryN(i).distance(queried.getGeometryN(j)));
            }
        }
        return minDis[n - 1][m - 1];
    }

    public static boolean judgeDistance(Geometry search, Geometry queried, double threshold) {
        return calulateDistance(search,queried) <= threshold;
    }
}
