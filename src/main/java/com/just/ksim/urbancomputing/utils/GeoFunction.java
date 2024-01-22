package com.just.ksim.urbancomputing.utils;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.Arrays;
import java.util.List;

/**
 * 地理信息相关计算方法
 *
 * @author ruansijie
 * @author moxiongjian
 * @date 2019-03-18 10:19
 */
public class GeoFunction {

    /**
     * 地球长半径
     */
    public static final double EARTH_RADIUS_IN_METER = 6378137.0;

    /**
     * 扁率
     */
    public static final double FLATTENING = 0.00669342162296594323;

    /**
     * 计算两经纬度间距离
     *
     * @param lng1 起始点经度
     * @param lat1 起始点纬度
     * @param lng2 终点经度
     * @param lat2 终点纬度
     * @return kilometer
     */
    public static double getDistanceInKM(double lng1, double lat1, double lng2, double lat2) {
        return getDistanceInM(lng1, lat1, lng2, lat2) / 1000.0;
    }

    /**
     * 计算两经纬度间距离
     *
     * @param p1 第一个点
     * @param p2 第二个点
     * @return double kilometer
     */
    public static double getDistanceInKM(Point p1, Point p2) {
        CheckUtils.checkEmpty(p1, p2);
        return getDistanceInKM(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    /**
     * 计算两经纬度间距离
     *
     * @param lng1 起始点经度
     * @param lat1 起始点纬度
     * @param lng2 终点经度
     * @param lat2 终点纬度
     * @return meter
     */
    public static double getDistanceInM(double lng1, double lat1, double lng2, double lat2) {
        double radLat1 = Math.toRadians(lat1);
        double radLat2 = Math.toRadians(lat2);
        double radLatDistance = radLat1 - radLat2;
        double radLngDistance = Math.toRadians(lng1) - Math.toRadians(lng2);
        return 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(radLatDistance / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(radLngDistance / 2), 2))) * EARTH_RADIUS_IN_METER;
    }

    /**
     * 米勒坐标投影，将经纬度坐标转平面坐标
     * 文档链接 ：https://blog.csdn.net/qq_31100961/article/details/52331708?locationNum=2&fps=1
     *
     * @param lng lng
     * @param lat lat
     * @return tuple，第一个元素是lng，第二个元素是lat
     */
    public static double[] gcsToMiller(double lng, double lat) {
        double width = 2 * Math.PI * EARTH_RADIUS_IN_METER;
        double height = 0.5 * width;
        double mill = 2.3;
        double x = Math.toRadians(lng);
        double y = Math.toRadians(lat);
        y = 1.25 * Math.log(Math.tan(0.25 * Math.PI + 0.4 * y));
        x = (width / 2) + (width / (2 * Math.PI)) * x;
        y = (height / 2) - (height / (2 * mill)) * y;
        return new double[]{x, y};
    }

    /**
     * 计算两经纬度间距离
     *
     * @param p1 第一个点
     * @param p2 第二个点
     * @return double meter
     */
    public static double getDistanceInM(Point p1, Point p2) {
        CheckUtils.checkEmpty(p1, p2);
        return getDistanceInM(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    /**
     * 计算一个 point list 对应的长度, 单位为M
     *
     * @param points point list
     * @return 长度
     */
    public static double getDistanceInM(List<Point> points) {
        double dist = 0;
        if (points.size() < 2) {
            return dist;
        }
        for (int i = 1; i < points.size(); i++) {
            dist += GeoFunction.getDistanceInM(points.get(i), points.get(i - 1));
        }
        return dist;
    }

    /**
     * 将球面距离转化为度（在地理坐标系下做缓冲区时使用）
     * 此方法与经纬度转距离的前提一致，即将地球抽象为规则球体
     * 注意:此方法南北向无误差，东西向缓冲距离略小
     *
     * @param distance 距离，单位m
     * @return 弧度
     */
    public static double getDegreeFromM(double distance) {
        double perimeter = 2 * Math.PI * EARTH_RADIUS_IN_METER;
        double degreePerM = 360 / perimeter;
        return distance * degreePerM;
    }

    /**
     * 计算两点之间的欧几里得距离
     *
     * @param point1 第一个点
     * @param point2 第二个点
     * @return double 欧几里得距离
     */
    public static double getEuclideanDis(Point point1, Point point2) {
        CheckUtils.checkEmpty(point1, point2);
        double x = point1.getX() - point2.getX();
        double y = point1.getY() - point2.getY();
        return Math.sqrt(x * x + y * y);
    }

    /**
     * 计算两点之间的欧几里得距离
     *
     * @param lng1 经度
     * @param lat1 纬度
     * @param lng2 经度
     * @param lat2 纬度
     * @return 返回欧氏距离，注意这里返回的还是经纬度弧度数
     */
    public static double getEuclideanDis(double lng1, double lat1, double lng2, double lat2) {
        double x = lng1 - lng2;
        double y = lat1 - lat2;
        return Math.sqrt(x * x + y * y);
    }

    /**
     * 给定两个点，计算两点之间的速度。两点有先后顺序，因此可能返回负值
     *
     * @param p1 第一个点
     * @param p2 第二个点
     * @return 速度，m/s
     */
    public static double getSpeed(Point p1, Point p2) {
        long timeSpanInSecond = (long) (p2.getCoordinate().getZ() - p1.getCoordinate().getZ()) / 1000;
        if (timeSpanInSecond == 0) {
            return 0;
        }
        double distanceInMeter = GeoFunction.getDistanceInM(p1, p2);
        return distanceInMeter / timeSpanInSecond;
    }

    /**
     * 计算两个点之间的倾斜角度
     *
     * @param startPt 点
     * @param endPt   点
     * @return 倾斜角度
     */
    private static double bearing(Point startPt, Point endPt) {
        double ptALatRad = Math.toRadians(startPt.getY());
        double ptALngRad = Math.toRadians(startPt.getX());
        double ptBLatRad = Math.toRadians(endPt.getY());
        double ptBLngRad = Math.toRadians(endPt.getX());
        double y = Math.sin(ptBLngRad - ptALngRad) * Math.cos(ptBLatRad);
        double x = Math.cos(ptALatRad) * Math.sin(ptBLatRad) - Math.sin(ptALatRad) * Math.cos(ptBLatRad) * Math.cos(ptBLngRad - ptALngRad);
        double bearingRad = Math.atan2(y, x);
        return (Math.toDegrees(bearingRad) + 360.0) % 360.0;
    }

    /**
     * 输入wktString，将polygon的坐标转换后求面积
     *
     * @param wktString 原始polygon的 wkt string
     * @return polygon的面积，单位为平方米
     * @throws Exception 只支持polygon
     */
    public static double getPolygonArea(String wktString) throws Exception {
        Geometry geometry = WKTUtils.read(wktString);
        if (!geometry.getClass().equals(Polygon.class)) {
            throw new Exception("only support polygon geometry");
        }
        Polygon polygon = (Polygon) WKTUtils.read(wktString);
        return getPolygonArea(polygon);
    }

    /**
     * 重载，输入geometry，将geometry的坐标转换后求面积
     *
     * @param geometry polygon
     * @return 面积 单位为平方米
     */
    public static double getPolygonArea(Geometry geometry) {
        Coordinate[] coordinates = Arrays.stream(geometry.getBoundary().getCoordinates()).map(GeoFunction::transform).toArray(Coordinate[]::new);
        return geometry.getFactory().createPolygon(coordinates).getArea();
    }

    /**
     * 坐标转换的辅助方程
     *
     * @param coordinate 坐标
     * @return 转换后的坐标
     */
    private static Coordinate transform(Coordinate coordinate) {
        double[] convertedCoord = gcsToMiller(coordinate.x, coordinate.y);
        return new Coordinate(convertedCoord[0], convertedCoord[1]);
    }

}
