package com.just.ksim.entity;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTWriter;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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
            this.dpFeature.getPivot();
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
        private List<Tuple2<Polygon, List<Geometry>>> pivot;
        private MultiPolygon polygons;
        private Set<Integer> indexes = new HashSet<>(2);
        private List<Integer> indexesList = new ArrayList<>(2);
        private boolean sort = false;

        public DPFeature() {
        }

        public List<Integer> getIndexes() {
            if (!sort) {
                getPivot();
                sort = true;
                indexesList = indexes.stream().sorted().collect(Collectors.toCollection(ArrayList::new));
            }
            return indexesList;
        }

        public MultiPolygon getMBRs() {
            //this.pivot.stream().map(v -> v._1).toArray()
            if (null == polygons) {
                getPivot();
                Polygon[] ps = this.pivot.stream().map(v -> v._1).toArray(Polygon[]::new);
                polygons = new MultiPolygon(ps, pre, 4326);
            }
            //MultiPolygon multiPolygon = new MultiPolygon(ps, pre, 4326);
            return polygons;
        }

        public DPFeature(List<Tuple2<Polygon, List<Geometry>>> pivot) {
            this.pivot = pivot;
        }

        public List<Tuple2<Polygon, List<Geometry>>> getPivot() {
            if (null == this.pivot) {
                dp(0, getNumGeometries() - 1);
            }
            return this.pivot;
        }

        public boolean obtuseAngle(Coordinate p1, Coordinate p2, Coordinate p3) {
            double l1X = p2.x - p1.x;
            double l1Y = p2.y - p1.y;
            double l2X = p3.x - p1.x;
            double l2Y = p3.y - p1.y;
            return (l1X * l2X + l1Y * l2Y) < 0;
        }

        public void dp(int startPoint, int endPoint) {
            LineSegment lineSegment = new LineSegment(getGeometryN(startPoint).getCoordinate(), getGeometryN(endPoint).getCoordinate());
            double maxDis = 0.0;
            int currentIndex = startPoint;
            int currentMinXIndex = startPoint;
            int currentMaxXIndex = endPoint;
            double currentMinXValue = 44444;
            double currentMaxXValue = 0.0;
            Coordinate sp = getGeometryN(startPoint).getCoordinate();
            Coordinate ep = getGeometryN(endPoint).getCoordinate();
            if (sp.x > ep.x) {
                Coordinate tmp = sp;
                sp = ep;
                ep = tmp;
            }
            for (int i = startPoint + 1; i < endPoint; i++) {
                Coordinate p = getGeometryN(i).getCoordinate();
                double dis = lineSegment.distancePerpendicular(p);
                if (dis > maxDis) {
                    currentIndex = i;
                    maxDis = dis;
                }
                if (obtuseAngle(sp, ep, p)) {
                    double k = (ep.y - sp.y) / (ep.x - sp.x);
                    double k2 = Math.sqrt(k * k + 1);
                    double dsp = sp.distance(p);
                    double dsm = Math.sqrt(dsp * dsp - dis * dis);
                    double detalX = dsm / k2;
                    if (currentMinXValue > detalX) {
                        currentMinXValue = detalX;
                        currentMinXIndex = i;
                    }
                }
                if (obtuseAngle(ep, sp, p)) {
                    double k = (ep.y - sp.y) / (ep.x - sp.x);
                    double k2 = Math.sqrt(k * k + 1);
                    double dsp = ep.distance(p);
                    double dsm = Math.sqrt(dsp * dsp - dis * dis);
                    double detalX = dsm / k2;
                    if (currentMaxXValue < detalX) {
                        currentMaxXValue = detalX;
                        currentMaxXIndex = i;
                    }
                }
            }
            if (maxDis > precision) {
                dp(startPoint, currentIndex);
                dp(currentIndex, endPoint);
            } else {
                //maxDis += 0.002;
                if (currentIndex == startPoint) {
                    List<Geometry> p = new ArrayList<>(2);
                    p.add(getGeometryN(startPoint));
                    p.add(getGeometryN(endPoint));
                    Coordinate[] points = new Coordinate[4];
                    points[0] = getGeometryN(startPoint).getCoordinate();
                    points[1] = getGeometryN(endPoint).getCoordinate();
                    points[2] = getGeometryN(endPoint).getCoordinate();
                    points[3] = points[0];
                    LinearRing line = new LinearRing(points, pre, 4326);
                    Polygon polygon = new Polygon(line, null, pre, 4326);
                    indexes.add(startPoint);
                    indexes.add(endPoint);
                    add(new Tuple2<>(polygon, p));
                } else {
//                    Coordinate sp = getGeometryN(startPoint).getCoordinate();
//                    Coordinate ep = getGeometryN(endPoint).getCoordinate();
//                    if (sp.x > ep.x) {
//                        Coordinate tmp = sp;
//                        sp = ep;
//                        ep = tmp;
//                    }

                    if (sp.x == ep.x) {
                        Coordinate p = getGeometryN(currentIndex).getCoordinate();
                        Coordinate tmp1 = sp;
                        Coordinate tmp2 = ep;
                        if (p.y < sp.y && sp.y < ep.y) {
                            tmp1 = p;
                        }
                        if (p.y > ep.y && sp.y < ep.y) {
                            tmp2 = p;
                        }
                        if (p.y < ep.y && ep.y < sp.y) {
                            tmp2 = p;
                        }
                        if (p.y > sp.y && ep.y < sp.y) {
                            tmp1 = p;
                        }

                        Coordinate[] points = new Coordinate[5];
                        points[0] = new Coordinate(tmp1.x - maxDis, tmp1.y);
                        points[1] = new Coordinate(tmp1.x + maxDis, tmp1.y);
                        points[2] = new Coordinate(tmp2.x + maxDis, tmp2.y);
                        points[3] = new Coordinate(tmp2.x - maxDis, tmp2.y);
                        points[4] = new Coordinate(tmp1.x - maxDis, tmp1.y);
                        LinearRing line = new LinearRing(points, pre, 4326);
                        Polygon polygon = new Polygon(line, null, pre, 4326);
                        List<Geometry> ps = new ArrayList<>(3);
                        ps.add(getGeometryN(startPoint));
                        ps.add(getGeometryN(currentIndex));
                        ps.add(getGeometryN(endPoint));
                        indexes.add(startPoint);
                        indexes.add(endPoint);
                        add(new Tuple2<>(polygon, ps));
                    } else if (sp.y == ep.y) {
                        Coordinate[] points = new Coordinate[5];
                        Coordinate p = getGeometryN(currentIndex).getCoordinate();
                        Coordinate tmp1 = sp;
                        Coordinate tmp2 = ep;

                        if (p.x < sp.x && sp.x < ep.x) {
                            tmp1 = p;
                        }
                        if (p.x > ep.x && sp.x < ep.x) {
                            tmp2 = p;
                        }
                        if (p.x < ep.x && ep.x < sp.x) {
                            tmp2 = p;
                        }
                        if (p.x > sp.x && ep.x < sp.x) {
                            tmp1 = p;
                        }
                        double px1 = 0.0;
                        double py1 = 0.0;
                        double px2 = 0.0;
                        double py2 = 0.0;
                        if (currentMinXIndex != currentIndex && currentMinXIndex != startPoint) {
                            px1 = currentMinXValue;
                        }
                        if (currentMaxXIndex != currentIndex && currentMaxXIndex != endPoint) {
                            px2 = currentMaxXValue;
                        }
                        points[0] = new Coordinate(tmp1.x + px1, tmp1.y - maxDis);
                        points[1] = new Coordinate(tmp1.x + px1, tmp1.y + maxDis);
                        points[2] = new Coordinate(tmp2.x + px2, tmp2.y + maxDis);
                        points[3] = new Coordinate(tmp2.x + px2, tmp2.y - maxDis);
                        points[4] = new Coordinate(tmp1.x + px1, tmp1.y - maxDis);
                        LinearRing line = new LinearRing(points, pre, 4326);
                        Polygon polygon = new Polygon(line, null, pre, 4326);
                        List<Geometry> ps = new ArrayList<>(3);
                        ps.add(getGeometryN(startPoint));
                        ps.add(getGeometryN(currentIndex));
                        ps.add(getGeometryN(endPoint));
                        indexes.add(startPoint);
                        indexes.add(endPoint);
                        add(new Tuple2<>(polygon, ps));
                    } else {
                        Coordinate p = getGeometryN(currentIndex).getCoordinate();
                        double k = (ep.y - sp.y) / (ep.x - sp.x);
                        double k2 = Math.sqrt(k * k + 1);
                        double px1 = 0.0;
                        double py1 = 0.0;
                        double px2 = 0.0;
                        double py2 = 0.0;
                        if (currentMinXIndex != currentIndex && currentMinXIndex != startPoint) {
                            px1 = currentMinXValue;
                            py1 = px1 * k;
                        }
                        if (currentMaxXIndex != currentIndex && currentMaxXIndex != endPoint) {
                            px2 = currentMaxXValue;
                            py2 = px2 * k;
                        }
                        sp = new Coordinate(sp.x - px1, sp.y - py1);
                        ep = new Coordinate(ep.x + px2, ep.y + py2);
                        double dsp = sp.distance(p);
                        double dsm = Math.sqrt(dsp * dsp - maxDis * maxDis);
                        double detalX = dsm / k2;
                        double detalY = k * detalX;

                        double dsp2 = ep.distance(p);
                        double dsm2 = Math.sqrt(dsp2 * dsp2 - maxDis * maxDis);
                        double detalX2 = dsm2 / k2;
                        double detalY2 = k * detalX2;
                        double detalX3 = Math.abs(k * maxDis / k2);
                        double detalY3 = detalX3 / k;

                        Coordinate[] points = new Coordinate[5];

                        if (k > 0) {
                            if (p.x == sp.x) {
                                if (p.y > sp.y) {
                                    points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                    points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                    points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                    points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                    points[4] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                } else {
                                    points[0] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                    points[1] = p;
                                    points[2] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                    points[3] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                    points[4] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                }
                            } else if (p.x == ep.x) {
                                if (p.y > ep.y) {
                                    points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                    points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                    points[2] = p;
                                    points[3] = new Coordinate(p.x + 2 * detalX3, p.y - 2 * detalY3);
                                    points[4] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                } else {
                                    points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                    points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                    points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                    points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                    points[4] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                }
                            } else {
                                double pk = (p.y - sp.y) / (p.x - sp.x);
                                if (pk > -1.0 / k && pk < k) {
                                    if (p.x > sp.x + detalX) {
                                        points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                        points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                        if (p.x > ep.x + detalX2) {
                                            points[2] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                            points[3] = p;
                                        } else {
                                            points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                            points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        }
                                        points[4] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                    } else {
                                        points[0] = new Coordinate(p.x + 2 * detalX3, p.y - 2 * detalY3);
                                        points[1] = p;
                                        points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                        points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        points[4] = points[0];
                                    }
                                } else if (pk >= k) {
                                    if (p.x > sp.x) {
                                        points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                        points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                        if (p.x > ep.x - detalX2) {
                                            points[2] = p;
                                            points[3] = new Coordinate(p.x + 2 * detalX3, p.y - 2 * detalY3);
                                        } else {
                                            points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                            points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        }
                                        points[4] = points[0];
                                    } else {
                                        points[0] = p;
                                        points[1] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                        points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                        points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        points[4] = points[0];
                                    }
                                } else {
                                    if (p.x < sp.x) {
                                        points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                        points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                        points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                        points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        points[4] = points[0];
                                    } else {
                                        points[0] = p;
                                        points[1] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                        points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                        points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        points[4] = points[0];
                                    }
                                }
                            }
                        } else {
                            if (p.x == sp.x) {
                                if (p.y <= sp.y) {
                                    points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                    points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                    points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                    points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                    points[4] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                } else {
                                    points[0] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                    points[1] = p;
                                    points[2] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                    points[3] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                    points[4] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                }
                            } else if (p.x == ep.x) {
                                if (p.y <= ep.y) {
                                    points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                    points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                    points[2] = p;
                                    points[3] = new Coordinate(p.x + 2 * detalX3, p.y - 2 * detalY3);
                                    points[4] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                } else {
                                    points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                    points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                    points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                    points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                    points[4] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                }
                            } else {
                                double pk = (p.y - sp.y) / (p.x - sp.x);
                                if (pk < -1.0 / k && pk > k) {
                                    if (p.x > sp.x + detalX) {
                                        points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                        points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                        if (p.x > ep.x + detalX2) {
                                            points[2] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                            points[3] = p;
                                        } else {
                                            points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                            points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        }
                                        points[4] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                    } else {
                                        points[0] = new Coordinate(p.x + 2 * detalX3, p.y - 2 * detalY3);
                                        points[1] = p;
                                        points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                        points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        points[4] = points[0];
                                    }
                                } else if (pk >= 1.0 / k) {
                                    if (p.x < sp.x) {
                                        points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                        points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                        points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                        points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        points[4] = points[0];
                                    } else {
                                        points[0] = p;
                                        points[1] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                        points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                        points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        points[4] = points[0];
                                    }
                                } else {
                                    if (p.x > sp.x) {
                                        points[0] = new Coordinate(sp.x + detalX3, sp.y - detalY3);
                                        points[1] = new Coordinate(sp.x - detalX3, sp.y + detalY3);
                                        if (p.x > ep.x - detalX2) {
                                            points[2] = p;
                                            points[3] = new Coordinate(p.x + 2 * detalX3, p.y - 2 * detalY3);
                                        } else {
                                            points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                            points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        }
                                        points[4] = points[0];
                                    } else {
                                        points[0] = p;
                                        points[1] = new Coordinate(p.x - 2 * detalX3, p.y + 2 * detalY3);
                                        points[2] = new Coordinate(ep.x - detalX3, ep.y + detalY3);
                                        points[3] = new Coordinate(ep.x + detalX3, ep.y - detalY3);
                                        points[4] = points[0];
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
                        indexes.add(startPoint);
                        indexes.add(endPoint);
                        add(new Tuple2<>(polygon, pvs));
                    }
                }
            }
        }

        public void add(Tuple2<Polygon, List<Geometry>> p) {
            if (null == this.pivot) {
                this.pivot = new ArrayList<>();
            }
            this.pivot.add(p);
        }
    }
}