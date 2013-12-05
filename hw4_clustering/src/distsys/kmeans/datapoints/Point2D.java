package distsys.kmeans.datapoints;

import distsys.kmeans.Centroid;
import distsys.kmeans.DataPoint;

import java.text.DecimalFormat;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 12/3/13
 */
public class Point2D extends DataPoint {

    double x;
    double y;

    /**
     * DataPoint that represents a 2D point in space
     *
     * @param x X coordinate
     * @param y Y coordinate
     */
    public Point2D(double x, double y) {
        super(-1);
        this.x = x;
        this.y = y;
    }

    /**
     * Convert a Point2D into an equivalent Centroid with init cluster
     *
     * @return a new Point2DCentroid from this Point2D
     */
    @Override
    public Centroid dataPointToCentroid(int cluster) {
        return new Point2DCentroid(x, y, cluster);
    }


    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("#.###");
        return cluster + " - (" + df.format(x) + ", " + df.format(y) + ")";
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }
}