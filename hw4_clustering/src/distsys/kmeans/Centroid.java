package distsys.kmeans;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 12/3/13
 */
public interface Centroid {

    /**
     * Calculate the distance from this DataPoint to another
     *
     * @param point The DataPoint to be compared to the current one
     * @return The distance between points
     */
    public double distanceFrom(DataPoint point);

    /**
     * Given a list of DataPoints, iterate through to find points that belong to this Centroid's
     * cluster, and compute an average position from them
     *
     * @param points A list of all DataPoints to be considered
     */
    public void calculateNewCenter(List<DataPoint> points);

}
