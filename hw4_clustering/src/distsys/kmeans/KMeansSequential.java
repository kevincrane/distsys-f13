package distsys.kmeans;

import distsys.kmeans.datapoints.Point2D;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/3/13
 */
public class KMeansSequential {

    private int numPoints;
    private int numClusters;
    private List<DataPoint> dataPoints;
    private final double THRESHOLD = 0.001;


    // TODO: remove
    private void generateData() {
        dataPoints.add(new Point2D(2, 2));
        dataPoints.add(new Point2D(-1, 3.5));
        dataPoints.add(new Point2D(3, 1));
        dataPoints.add(new Point2D(-1, -2));
        dataPoints.add(new Point2D(-2, 2.5));
        dataPoints.add(new Point2D(1, -1));
        dataPoints.add(new Point2D(3, 3.5));
        dataPoints.add(new Point2D(-3, 3));
        dataPoints.add(new Point2D(4, 3));
        dataPoints.add(new Point2D(2, -2));
    }


    /**
     * Class for performing a K-Means clustering algorithm across a set of DataPoints
     * (Sequential version)
     *
     * @param numPoints   Number of DataPoints
     * @param numClusters Number of Clusters to find
     */
    public KMeansSequential(int numPoints, int numClusters) {
        this.numPoints = numPoints;
        this.numClusters = numClusters;
        this.dataPoints = new ArrayList<DataPoint>(numPoints);
    }

    public void setDataPoints(List<DataPoint> dataPoints) {
        this.dataPoints = dataPoints;
    }


    /**
     * Pick out a number of random DataPoints to act as initial Centroids
     *
     * @return a list of Centroids spread far from each other
     */
    private List<Centroid> chooseInitialCentroids() {
        List<Centroid> centroids = new ArrayList<Centroid>(numClusters);
        Set<Integer> usedPoints = new HashSet<Integer>();
        Random random = new Random();
        int nextCluster = 0;

        while (centroids.size() < numClusters) {
            // Choose random datapoint to use as centroid
            int nextPoint = random.nextInt(numPoints);
            if (usedPoints.contains(nextPoint)) {
                // Skip any point that's already been added
                continue;
            }

            // Add centroid to list
            centroids.add(dataPoints.get(nextPoint).dataPointToCentroid(nextCluster));
            usedPoints.add(nextPoint);
            nextCluster++;

            System.out.println("Chose initial centroid as " + centroids.get(nextCluster - 1) + "!!"); //TODO remove
        }

        return centroids;
    }

    /**
     * Execute the K-Means clustering algorithm
     */
    public void findClusters() {
        System.out.println("Forming " + numClusters + " clusters using K-Means..");
//        generateData();     //TODO replace with something better, in ClusterMain probably

        // Randomly select initial centroids
        List<Centroid> centroids = chooseInitialCentroids();
        int iterations = 0;
        int numPointsChanged = Integer.MAX_VALUE;

        // Repeat until the number of points that changes clusters exceeds a certain threshold (0.1% of points)
        while ((((double) numPointsChanged) / numPoints) > THRESHOLD) {
            numPointsChanged = 0;
            iterations++;

            // Find the nearest centroid for each DataPoint
            for (DataPoint point : dataPoints) {
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

                // Update the DataPoint's cluster if it changed
                if (point.getCluster() != closestCluster) {
                    point.setCluster(closestCluster);
                    numPointsChanged++;
                }
//                System.out.println("DataPoint " + point);       //TODO remove
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

        // Sort everything into their final clusters
        List<List<DataPoint>> clusteredData = new ArrayList<List<DataPoint>>(numClusters);
    }

}
