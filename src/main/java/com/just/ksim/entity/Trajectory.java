package com.just.ksim.entity;

import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-02-20 18:37
 * @modified by :
 **/
public class Trajectory extends MultiPoint {

    private String id;

    public Trajectory(String id, Point[] points, PrecisionModel precisionModel, int SRID) {
        super(points, precisionModel, SRID);
        this.id = id;
    }
//
//    public Trajectory(String id, CoordinateSequence points, GeometryFactory factory) {
//        super(points, factory);
//        this.id = id;
//    }


    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return id + "-" + toText();
    }
}
