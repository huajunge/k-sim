package com.just.ksim.preprocess;

import com.just.ksim.entity.Trajectory;
import org.locationtech.jts.geom.Point;
import util.PreprocessorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : hehuajun3
 * @description : HeuristicFilter
 * @date : Created in 2021-03-15 11:02
 * @modified by :
 **/
public class HeuristicFilterAndSegment implements Serializable {
    /**
     * 每秒最大速度
     */
    private double maxSpeedMeterPerSecond;

    private int maxTimeInterval;

    public HeuristicFilterAndSegment(double maxSpeedMeterPerSecond, int maxTimeInterval) {
        this.maxSpeedMeterPerSecond = maxSpeedMeterPerSecond;
        this.maxTimeInterval = maxTimeInterval * 1000;
    }

    public List<Trajectory> filter(Trajectory rawTraj) {
        List<Trajectory> cleanedTrajectoryList = new ArrayList<>();
        List<Point> cleanedPtList = new ArrayList<>();
        Point prePt = null;
        for (int i = 0; i < rawTraj.getNumGeometries(); i++) {
            Point curPt = (Point) rawTraj.getGeometryN(i);
            if (null == prePt) {
                cleanedPtList.add(curPt);
                prePt = curPt;
            } else {
                if (curPt.getCoordinate().getZ() > prePt.getCoordinate().getZ()) {
                    if (PreprocessorUtils.isNormalSpeedPoint(prePt, curPt, maxSpeedMeterPerSecond) && (curPt.getCoordinate().getZ() - prePt.getCoordinate().getZ()) <= maxTimeInterval) {
                        cleanedPtList.add(curPt);
                        prePt = curPt;
                    } else {  // the timestamp of curPt is not increasing or the speed exceeds threshold, skip it.
                        // construct trajectoryfeature for previous cleanedPtList
                        Trajectory filteredTraj = PreprocessorUtils.formNewTrajectory(rawTraj.getId(), cleanedPtList);
                        if (filteredTraj != null) {
                            cleanedTrajectoryList.add(filteredTraj);
                        }
                        cleanedPtList = new ArrayList<>();
                        prePt = null;
                    }
                }
            }
        }

        Trajectory filteredTraj = PreprocessorUtils.formNewTrajectory(rawTraj.getId(), cleanedPtList);
        if (filteredTraj != null) {
            cleanedTrajectoryList.add(filteredTraj);
        }
        return cleanedTrajectoryList;
    }
}
