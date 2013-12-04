package distsys.kmeans.datapoints;

import distsys.kmeans.Centroid;
import distsys.kmeans.DataPoint;

import java.util.List;

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
    public Point2DCentroid(double x, double y, int cluster) {
        super(x, y);
        this.cluster = cluster;
    }

    /**
     * Compute the Euclidean distance between the two 2D points
     *
     * @param point The Point2D to be compared to this centroid
     * @return The distance between the two points
     */
    @Override
    public double distanceFrom(DataPoint point) {
        if (!(point instanceof Point2D)) {
            System.err.println("Error: can't compare distance between types Point2D and " + point.getClass().getName());
            return -1;
        }
        Point2D point2D = (Point2D) point;
        return Math.sqrt(Math.pow(x - point2D.getX(), 2) + Math.pow(y - point2D.getY(), 2));
    }

    /**
     * Given a list of Point2Ds, iterate through to find points that belong to this Centroid's
     * cluster, and average their (x,y) coordinates to determine the new center point
     *
     * @param points A list of all Point2Ds to be considered
     */
    @Override
    public void calculateNewCenter(List<DataPoint> points) {
        double totalX = 0;
        double totalY = 0;
        int totalPoints = 0;

        for (DataPoint point : points) {
            // First typecheck the DataPoint to be sure it's a Point2D
            if (!(point instanceof Point2D)) {
                continue;
            }

            // If this point belongs to the right cluster, add the (x,y) coords
            if (point.getCluster() == cluster) {
                totalX += ((Point2D) point).getX();
                totalY += ((Point2D) point).getY();
                totalPoints++;
            }
        }

        // Set the new X and Y coordinates of the centroid to the average of the points
        x = (totalPoints == 0) ? 0 : (totalX / totalPoints);
        y = (totalPoints == 0) ? 0 : (totalY / totalPoints);

        System.out.println("New centroid location " + this);
    }
}
