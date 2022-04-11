package just.urbancomputing.similarity;

import org.locationtech.jts.geom.Geometry;
import utils.NumberUtil;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-10 12:06
 * @modified by :
 **/
public class DTW {
    public static double calulateDistance(Geometry search, Geometry queried) {
        int n = search.getNumGeometries();
        int m = queried.getNumGeometries();
        double[][] minDis = new double[n][m];
        minDis[0][0] = search.getGeometryN(0).distance(queried.getGeometryN(0));

        for (int i = 1; i < n; i++) {
            minDis[i][0] = minDis[i - 1][0] + search.getGeometryN(i).distance(queried.getGeometryN(0));
        }

        for (int j = 1; j < m; j++) {
            minDis[0][j] = minDis[0][j - 1] + search.getGeometryN(0).distance(queried.getGeometryN(j));
        }

        for (int i = 1; i < n; i++) {
            for (int j = 1; j < m; j++) {
                minDis[i][j] = search.getGeometryN(i).distance(queried.getGeometryN(j)) + NumberUtil.min(minDis[i - 1][j], minDis[i][j - 1], minDis[i - 1][j - 1]);
            }
        }
        return minDis[n - 1][m - 1];
    }
}
