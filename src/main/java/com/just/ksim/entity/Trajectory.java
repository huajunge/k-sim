package com.just.ksim.entity;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.WKTWriter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-02-20 18:37
 * @modified by :
 **/
public class Trajectory implements Serializable {

    private String id;

    private MultiPoint multiPoint;

//    public Trajectory(String id, Point[] points, PrecisionModel precisionModel, int SRID) {
//        super(points, precisionModel, SRID);
//        this.id = id;
//    }

    public Trajectory(String id, MultiPoint multiPoint) {
        this.id = id;
        this.multiPoint = multiPoint;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Trajectory(String id, List<Point> pointList) {
        this.id = id;
        this.multiPoint = new MultiPoint(pointList.toArray(new Point[0]), new PrecisionModel(), 4326);
    }
//
//    public Trajectory(String id, CoordinateSequence points, GeometryFactory factory) {
//        super(points, factory);
//        this.id = id;
//    }


    public String getId() {
        return id;
    }

    public MultiPoint getMultiPoint() {
        return multiPoint;
    }

    public Geometry getGeometryN(int i) {
        return this.multiPoint.getGeometryN(i);
    }

    public int getNumGeometries() {
        return this.multiPoint.getNumGeometries();
    }

    public String toText() {
        return this.multiPoint.toText();
    }

    @Override
    public String toString() {
        WKTWriter writer = new WKTWriter(3);
        //writer.write(this.multiPoint);
        return id + "-" + writer.write(this.multiPoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, multiPoint);
    }
}
