package util;

import com.just.ksim.entity.Trajectory;
import org.locationtech.jts.geom.Point;

import java.util.List;


/**
 * Description:
 *
 * @author : Sijie Ruan
 * @date : 2018/02/10
 */
public class PreprocessorUtils {

    /**
     * @param rawTrajectoryID 原始轨迹id
     * @param cleanedPtList   清洗的轨迹点
     * @return Trajectory
     */
    public static Trajectory formNewTrajectory(String rawTrajectoryID, List<Point> cleanedPtList) {
        if (cleanedPtList.size() > 0) {
            long timeHash = (long) (cleanedPtList.get(0).getCoordinate().getZ() / 1000);
            return new Trajectory(rawTrajectoryID + "_" + timeHash, cleanedPtList);
        }  else {
            return null;
        }
    }

    /**
     * 是不是正常轨迹速度点
     *
     * @param p1                     第一个点
     * @param p2                     第二个点
     * @param maxSpeedMeterPerSecond 最大速度
     * @return boolean true 正常, false不正常
     */
    public static boolean isNormalSpeedPoint(Point p1, Point p2, double maxSpeedMeterPerSecond) {
        double speed = GeoFunction.getSpeed(p1, p2);
        return speed <= maxSpeedMeterPerSecond;
    }
}
