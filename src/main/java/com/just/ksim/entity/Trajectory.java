package com.just.ksim.entity;

import org.locationtech.jts.geom.*;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-02-20 18:37
 * @modified by :
 **/
public class Trajectory extends LineString {

    private String id;

    public Trajectory(String id, Coordinate[] points, PrecisionModel precisionModel, int SRID) {
        super(points, precisionModel, SRID);
        this.id = id;
    }

    public Trajectory(String id, CoordinateSequence points, GeometryFactory factory) {
        super(points, factory);
        this.id = id;
    }

    @Override
    public String toString() {
        return "Trajectory{" +
                "id='" + id + '\'' +
                ", points=" + points +
                '}';
    }
}
