package distsys.kmeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/5/13
 */
public abstract class KMeans {

    protected int numPoints;
    protected int numClusters;
    protected List<DataPoint> dataPoints;
    protected List<Centroid> centroids;
    protected final double THRESHOLD = 0.001;
    protected final int MAX_ITERATIONS = 25;


    /**
     * Class for performing a K-Means clustering algorithm across a set of DataPoints
     *
     * @param numPoints   Number of DataPoints
     * @param numClusters Number of Clusters to find
     */
    public KMeans(int numPoints, int numClusters) {
        this.numPoints = numPoints;
        this.numClusters = numClusters;
        this.dataPoints = new ArrayList<DataPoint>(numPoints);
    }

    public void setDataPoints(List<DataPoint> dataPoints) {
        this.dataPoints = dataPoints;
    }

    public List<DataPoint> getDataPoints() {
        return dataPoints;
    }


    /**
     * Pick out a number of random DataPoints to act as initial Centroids
     *
     * @return a list of Centroids spread far from each other
     */
    protected List<Centroid> chooseInitialCentroids(List<DataPoint> allDataPoints) {
        List<Centroid> centroids = new ArrayList<Centroid>(numClusters);
        Random random = new Random();
        int nextCluster = 0;

        while (centroids.size() < numClusters) {
            // Add random centroid to list
            Centroid nextCentroid = allDataPoints.get(random.nextInt(numPoints)).dataPointToCentroid(nextCluster);
            if (centroids.contains(nextCentroid)) {
                // Skip any point that's already been added
                continue;
            }
            centroids.add(nextCentroid);
            nextCluster++;

            System.out.println("Chose initial centroid as " + centroids.get(nextCluster - 1) + "!!"); //TODO remove
        }

        return centroids;
    }

    /**
     * Find the nearest cluster number for a DataPoint
     *
     * @param point     DataPoint to search around
     * @param centroids List of current centroids
     * @return Cluster number of the centroid that is closest
     */
    protected int getClosestCluster(DataPoint point, List<Centroid> centroids) {
        int closestCluster = 0;
        double closestDistance = Double.MAX_VALUE;

        // Find the nearest cluster
        for (Centroid cent : centroids) {
            if (cent.distanceFrom(point) < closestDistance) {
                // Found a nearer centroid
                closestDistance = cent.distanceFrom(point);
                closestCluster = cent.getCluster();
            }
        }
        return closestCluster;
    }

    /**
     * Execute the K-Means clustering algorithm
     */
    public abstract void findClusters();

}
