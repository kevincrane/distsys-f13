package distsys.kmeans.datapoints;

import distsys.kmeans.Centroid;
import distsys.kmeans.DataPoint;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/3/13
 */
public class Point2DCentroid extends Point2D implements Centroid {


    /**
     * Centroid DataPoint that represents the average (x,y) of all points in its cluster
     *
     * @param x X coordinate
     * @param y Y coordinate
     */
    public Point2DCentroid(double x, double y) {
        super(x, y);
    }

    /**
     * Compute the Euclidean distance between the two 2D points
     * @param point    The Point2D to be compared to this centroid
     * @return The distance between the two points
     */
    public double distanceFrom(DataPoint point) {
        if(!(point instanceof Point2D)) {
            System.err.println("Error: can't compare distance between types Point2D and " + point.getClass().getName());
            return -1;
        }
        Point2D point2D = (Point2D)point;
        return Math.sqrt(Math.pow(x - point2D.getX(), 2) + Math.pow(y - point2D.getY(), 2));
    }
}
