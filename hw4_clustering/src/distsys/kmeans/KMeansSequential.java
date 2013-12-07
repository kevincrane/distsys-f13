package distsys.kmeans;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/3/13
 */
public class KMeansSequential extends KMeans {

    /**
     * Class for performing a K-Means clustering algorithm across a set of DataPoints
     * (Sequential version)
     *
     * @param numPoints   Number of DataPoints
     * @param numClusters Number of Clusters to find
     */
    public KMeansSequential(int numPoints, int numClusters) {
        super(numPoints, numClusters);
    }


    /**
     * Execute the K-Means clustering algorithm
     */
    @Override
    public void findClusters() {
        System.out.println("Forming " + numClusters + " clusters from " + numPoints + " datapoints using K-Means..");

        // Randomly select initial centroids
        centroids = chooseInitialCentroids(dataPoints);
        int iterations = 0;
        int numPointsChanged = Integer.MAX_VALUE;

        // Repeat until the number of points that changes clusters exceeds a certain threshold (0.1% of points)
        while ((((double) numPointsChanged) / numPoints) > THRESHOLD && iterations < MAX_ITERATIONS) {
            numPointsChanged = 0;
            iterations++;

            for (DataPoint point : dataPoints) {
                // Find the nearest centroid for each DataPoint
                int closestCluster = getClosestCluster(point, centroids);

                // Update the DataPoint's cluster if it changed
                if (point.getCluster() != closestCluster) {
                    point.setCluster(closestCluster);
                    numPointsChanged++;
                }
//                System.out.println("DataPoint " + point);       //TODO for debugging
            }

            // Update the position of each centroid
            for (Centroid cent : centroids) {
                cent.calculateNewCenter(dataPoints);
            }

            System.out.println("Iteration " + iterations + ": changed " + numPointsChanged + " points.\n");
        }

        // All done!!
        System.out.println("Completed k-means for " + numClusters + " clusters of " + numPoints +
                " data points in " + iterations + " iterations.");
    }

}
