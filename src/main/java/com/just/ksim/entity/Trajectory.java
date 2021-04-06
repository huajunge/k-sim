package com.just.ksim.entity;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTWriter;
import scala.Tuple2;

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
    private DPFeature dpFeature = null;
    private MultiPoint multiPoint;
    private PrecisionModel pre = new PrecisionModel();
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

    public DPFeature getDPFeature() {
        if (null == this.dpFeature) {
            this.dpFeature = new DPFeature();
            this.dpFeature.getPviot();
        }
        return this.dpFeature;
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

    public class DPFeature implements Serializable {
        private double precision = 0.02;
        private List<Tuple2<Geometry, List<Geometry>>> pviot;

        public DPFeature() {
        }

        public DPFeature(List<Tuple2<Geometry, List<Geometry>>> pviot) {
            this.pviot = pviot;
        }

        public List<Tuple2<Geometry, List<Geometry>>> getPviot() {
            if (null == this.pviot) {
                dp(0, getNumGeometries() - 1);
            }
            return this.pviot;
        }

        public void dp(int startPoint, int endPoint) {
            LineSegment lineSegment = new LineSegment(getGeometryN(startPoint).getCoordinate(), getGeometryN(endPoint).getCoordinate());
            double maxDis = 0.0;
            int currentIndex = startPoint;
            for (int i = startPoint + 1; i < endPoint; i++) {
                double dis = lineSegment.distance(getGeometryN(i).getCoordinate());
                if (dis > maxDis) {
                    currentIndex = i;
                    maxDis = dis;
                }
            }
            if (maxDis > precision) {
                dp(startPoint, currentIndex);
                dp(currentIndex, endPoint);
            } else {
                if (currentIndex == startPoint) {
                    List<Geometry> p = new ArrayList<>(2);
                    p.add(getGeometryN(startPoint));
                    p.add(getGeometryN(endPoint));
                    add(new Tuple2<>(null, p));
                } else {
                    Coordinate sp = getGeometryN(startPoint).getCoordinate();
                    Coordinate ep = getGeometryN(endPoint).getCoordinate();
                    if (sp.x == ep.x) {
                        Coordinate[] points = new Coordinate[5];
                        points[0] = new Coordinate(sp.x - maxDis, sp.y);
                        points[1] = new Coordinate(sp.x + maxDis, sp.y);
                        points[2] = new Coordinate(ep.x + maxDis, ep.y);
                        points[3] = new Coordinate(ep.x - maxDis, sp.y);
                        points[4] = new Coordinate(sp.x - maxDis, sp.y);
                        LinearRing line = new LinearRing(points, pre, 4326);
                        Polygon polygon = new Polygon(line, null, pre, 4326);
                        List<Geometry> p = new ArrayList<>(3);
                        p.add(getGeometryN(startPoint));
                        p.add(getGeometryN(currentIndex));
                        p.add(getGeometryN(endPoint));
                        add(new Tuple2<>(polygon, p));
                    } else if (sp.y == ep.y) {
                        Coordinate[] points = new Coordinate[5];
                        points[0] = new Coordinate(sp.x, sp.y - maxDis);
                        points[1] = new Coordinate(sp.x, sp.y + maxDis);
                        points[2] = new Coordinate(ep.x, ep.y + maxDis);
                        points[3] = new Coordinate(ep.x, ep.y - maxDis);
                        points[4] = new Coordinate(sp.x, sp.y - maxDis);
                        LinearRing line = new LinearRing(points, pre, 4326);
                        Polygon polygon = new Polygon(line, null, pre, 4326);
                        List<Geometry> p = new ArrayList<>(3);
                        p.add(getGeometryN(startPoint));
                        p.add(getGeometryN(currentIndex));
                        p.add(getGeometryN(endPoint));
                        add(new Tuple2<>(polygon, p));
                    } else {
                        if (sp.x > ep.x) {
                            Coordinate tmp = sp;
                            sp = ep;
                            ep = tmp;
                        }
                        Coordinate p = getGeometryN(currentIndex).getCoordinate();
                        double k = (ep.y - sp.y) / (ep.x - sp.x);
                        double k2 = Math.sqrt(k * k + 1);
                        double dsp = sp.distance(p);
                        double dsm = Math.sqrt(dsp * dsp - maxDis * maxDis);
                        double detalX = dsm / k2;
                        double detalY = k * detalX;

                        double dsp2 = ep.distance(p);
                        double dsm2 = Math.sqrt(dsp2 * dsp2 - maxDis * maxDis);
                        double detalX2 = dsm2 / k2;
                        double detalY2 = k * detalX2;
                        double detalY3 = maxDis * 2 / k2;
                        double detalX3 = k * detalY3;

                        Coordinate[] points = new Coordinate[5];

                        if (k > 0) {
                            if (p.x == sp.x) {
                                if (p.y > sp.y) {
                                    points[0] = sp;
                                    points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                    points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                    points[3] = ep;
                                    points[4] = sp;
                                } else {
                                    points[0] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                    points[1] = p;
                                    points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                    points[3] = ep;
                                    points[4] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                }
                            } else if (p.x == ep.x) {
                                if (p.y > ep.y) {
                                    points[0] = sp;
                                    points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                    points[2] = p;
                                    points[3] = new Coordinate(ep.x + detalX2, ep.y + detalY2);
                                    points[4] = sp;
                                } else {
                                    points[0] = sp;
                                    points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                    points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                    points[3] = ep;
                                    points[4] = sp;
                                }
                            } else {
                                double pk = (p.y - sp.y) / (p.x - sp.x);
                                if (pk > -1.0 / k && pk < k) {
                                    if (p.x > sp.x + detalX) {
                                        points[0] = sp;
                                        points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                        if (p.x > ep.x + detalX2) {
                                            points[2] = p;
                                            points[3] = new Coordinate(ep.x + detalX2, ep.y + detalY2);
                                        } else {
                                            points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                            points[3] = ep;
                                        }
                                        points[4] = sp;
                                    } else {
                                        points[0] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                        points[1] = p;
                                        points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                        points[3] = ep;
                                        points[4] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                    }
                                } else if (pk >= k) {
                                    if (p.x > sp.x) {
                                        points[0] = sp;
                                        points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                        if (p.x > ep.x - detalX2) {
                                            points[2] = p;
                                            points[3] = new Coordinate(ep.x + detalX2, ep.y + detalY2);
                                        } else {
                                            points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                            points[3] = ep;
                                        }
                                        points[4] = sp;
                                    } else {
                                        points[0] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                        points[1] = p;
                                        points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                        points[3] = ep;
                                        points[4] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                    }
                                } else {
                                    if (p.x < sp.x) {
                                        points[0] = sp;
                                        points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                        points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                        points[3] = ep;
                                        points[4] = sp;
                                    } else {
                                        points[0] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                        points[1] = p;
                                        points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                        points[3] = ep;
                                        points[4] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                    }
                                }
                            }
                        } else {
                            if (p.x == sp.x) {
                                if (p.y > sp.y) {
                                    points[0] = sp;
                                    points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                    points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                    points[3] = ep;
                                    points[4] = sp;
                                } else {
                                    points[0] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                    points[1] = p;
                                    points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                    points[3] = ep;
                                    points[4] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                }
                            } else if (p.x == ep.x) {
                                if (p.y > ep.y) {
                                    points[0] = sp;
                                    points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                    points[2] = p;
                                    points[3] = new Coordinate(ep.x + detalX2, ep.y + detalY2);
                                    points[4] = sp;
                                } else {
                                    points[0] = sp;
                                    points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                    points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                    points[3] = ep;
                                    points[4] = sp;
                                }
                            } else {
                                double pk = (p.y - sp.y) / (p.x - sp.x);
                                if (pk < -1.0 / k && pk > k) {
                                    if (p.x > sp.x + detalX) {
                                        points[0] = sp;
                                        points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                        if (p.x > ep.x + detalX2) {
                                            points[2] = p;
                                            points[3] = new Coordinate(ep.x + detalX2, ep.y + detalY2);
                                        } else {
                                            points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                            points[3] = ep;
                                        }
                                        points[4] = sp;
                                    } else {
                                        points[0] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                        points[1] = p;
                                        points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                        points[3] = ep;
                                        points[4] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                    }
                                } else if (pk >= 1.0 / k) {
                                    if (p.x < sp.x) {
                                        points[0] = sp;
                                        points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                        points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                        points[3] = ep;
                                        points[4] = sp;
                                    } else {
                                        points[0] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                        points[1] = p;
                                        points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                        points[3] = ep;
                                        points[4] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                    }
                                } else {
                                    if (p.x > sp.x) {
                                        points[0] = sp;
                                        points[1] = new Coordinate(p.x - detalX, p.y - detalY);
                                        if (p.x > ep.x - detalX2) {
                                            points[2] = p;
                                            points[3] = new Coordinate(ep.x + detalX2, ep.y + detalY2);
                                        } else {
                                            points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                            points[3] = ep;
                                        }
                                        points[4] = sp;
                                    } else {
                                        points[0] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                        points[1] = p;
                                        points[2] = new Coordinate(p.x + detalX2, p.y + detalY2);
                                        points[3] = ep;
                                        points[4] = new Coordinate(sp.x - detalX, sp.y - detalY);
                                    }
                                }
                            }
                        }
                        LinearRing line = new LinearRing(points, pre, 4326);
                        Polygon polygon = new Polygon(line, null, pre, 4326);
                        List<Geometry> pvs = new ArrayList<>(3);
                        pvs.add(getGeometryN(startPoint));
                        pvs.add(getGeometryN(currentIndex));
                        pvs.add(getGeometryN(endPoint));
                        add(new Tuple2<>(polygon, pvs));
                    }
                }
            }
        }

        public void add(Tuple2<Geometry, List<Geometry>> p) {
            if (null == this.pviot) {
                this.pviot = new ArrayList<>();
            }
            this.pviot.add(p);
        }
    }
}