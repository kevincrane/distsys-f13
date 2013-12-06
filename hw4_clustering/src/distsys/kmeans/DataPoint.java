package distsys.kmeans;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/3/13
 */
public abstract class DataPoint implements Serializable {

    protected int cluster;        // Which cluster this data point belongs to

    /**
     * Abstract class to represent a data point that will be clustered.
     * Initially places each DataPoint in cluster -1 until it is formally sorted
     */
    protected DataPoint(int cluster) {
        this.cluster = cluster;
    }

    /**
     * Convert a DataPoint into an equivalent Centroid with init cluster
     *
     * @return a new Centroid from this DataPoint
     */
    public abstract Centroid dataPointToCentroid(int cluster);

    public int getCluster() {
        return cluster;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public abstract String toString();

    public abstract boolean equals(Object obj);

}
