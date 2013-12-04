package distsys.kmeans;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/3/13
 */
public abstract class DataPoint {

    protected int cluster;        // Which cluster this data point belongs to

    /**
     * Abstract class to represent a data point that will be clustered.
     * Initially places each DataPoint in cluster -1 until it is formally sorted
     */
    public DataPoint() {
        this.cluster = -1;
    }

    public int getCluster() {
        return cluster;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

}
