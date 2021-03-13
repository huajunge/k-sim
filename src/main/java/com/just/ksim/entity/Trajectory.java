package com.just.ksim.entity;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-02-20 18:37
 * @modified by :
 **/
public class Trajectory {

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
        return id + "-" + this.multiPoint.toText();
    }
}
