package distsys.kmeans;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 12/3/13
 */
public interface Centroid {

    /**
     * Calculate the distance from this DataPoint to another
     * @param point    The DataPoint to be compared to the current one
     * @return The distance between points
     */
    public double distanceFrom(DataPoint point);

}
