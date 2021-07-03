package com.just.ksim.entity;

import java.util.List;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-06-09 16:52
 * @modified by :
 **/
public class EnlargedElement {
    private double xMin;
    private double yMin;
    private double xMax;
    private double yMax;
    private double yLength;
    private double xLength;
    private int level;

    public EnlargedElement(double xMin, double yMin, double xMax, double yMax, int level) {
        this.xMin = xMin;
        this.yMin = yMin;
        this.xMax = xMax;
        this.yMax = yMax;
        this.level = level;
        this.xLength = xMax - xMin;
        this.yLength = yMax - yMin;
    }
}
