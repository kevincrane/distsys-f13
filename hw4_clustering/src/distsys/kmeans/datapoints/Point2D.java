package distsys.kmeans.datapoints;

import distsys.kmeans.DataPoint;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 12/3/13
 */
public class Point2D extends DataPoint {

    protected double x;
    protected double y;

    /**
     * DataPoint that represents a 2D point in space
     * @param x    X coordinate
     * @param y    Y coordinate
     */
    public Point2D(double x, double y) {
        super();
        this.x = x;
        this.y = y;
    }


    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }
}