package just.urbancomputing.similarity;

import just.urbancomputing.utils.NumberUtil;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;

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
            minDis[i][0] = Double.max(minDis[i-1][0], search.getGeometryN(i).distance(queried.getGeometryN(0)));
        }

        for (int j = 1; j < m; j++) {
            minDis[0][j] = Double.max(minDis[0][j-1], search.getGeometryN(0).distance(queried.getGeometryN(j)));
        }

        for (int i = 1; i < n; i++) {
            for (int j = 1; j < m; j++) {
                minDis[i][j] = Double.max(NumberUtil.min(minDis[i - 1][j], minDis[i][j - 1], minDis[i - 1][j - 1]),
                        search.getGeometryN(i).distance(queried.getGeometryN(j)));
            }
        }
        return minDis[n - 1][m - 1];
    }

    public static double calulateDistance2(Geometry search, Geometry queried) {
        int n = search.getNumGeometries();
        int m = queried.getNumGeometries();
        //double[][] minDis = new double[n][m];
        //minDis[0][0] = search.getGeometryN(0).distance(queried.getGeometryN(0));
        List<List<Double>> minDisArray = new ArrayList<>(n);
        List<Double> zero = new ArrayList<>(m);

        for (int i = 0; i < m; i++) {
            zero.add(0.0);
        }

        for (int i = 0; i < n; i++) {
            minDisArray.add(new ArrayList<>(zero));
        }

        minDisArray.get(0).add(0, 0.0);
        minDisArray.get(0).set(0, search.getGeometryN(0).distance(queried.getGeometryN(0)));
        for (int i = 1; i < n; i++) {
            //minDis[i][0] = Double.max(minDis[i][0], search.getGeometryN(i).distance(queried.getGeometryN(0)));
            minDisArray.get(i).set(0, Double.max(minDisArray.get(i).get(0), search.getGeometryN(i).distance(queried.getGeometryN(0))));
        }

        for (int j = 1; j < m; j++) {
            minDisArray.get(0).set(j, Double.max(minDisArray.get(0).get(j), search.getGeometryN(0).distance(queried.getGeometryN(j))));
            //minDis[0][j] = Double.max(minDis[0][j], search.getGeometryN(0).distance(queried.getGeometryN(j)));
        }

        for (int i = 1; i < n; i++) {
            for (int j = 1; j < m; j++) {
                minDisArray.get(i).set(j, Double.max(NumberUtil.min(minDisArray.get(i - 1).get(j), minDisArray.get(i).get(j - 1), minDisArray.get(i - 1).get(j - 1)),
                        search.getGeometryN(i).distance(queried.getGeometryN(j))));
                //minDis[i][j] = Double.max(NumberUtil.min(minDis[i - 1][j], minDis[i][j - 1], minDis[i - 1][j - 1]),
                //       search.getGeometryN(i).distance(queried.getGeometryN(j)));
            }
        }
        //return minDis[n - 1][m - 1];
        return minDisArray.get(n - 1).get(m - 1);
    }
}
