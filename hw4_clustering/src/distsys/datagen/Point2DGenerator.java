package distsys.datagen;

import distsys.kmeans.DataPoint;
import distsys.kmeans.datapoints.Point2D;
import distsys.kmeans.datapoints.Point2DCentroid;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/4/13
 */
public class Point2DGenerator extends DataGenerator {

    private final int MAX_WIDTH = 100;  // Max width/height of coordinate plane
    private final double MAX_VARIANCE = 10;
    private final double minDistance = 3;
    private List<Point2D> initPoints;

    public Point2DGenerator(int numPoints, int numClusters) {
        super(numPoints, numClusters);
        initPoints = new ArrayList<Point2D>(numClusters);

    }


    /**
     * Generate initial random centroid starting spots
     */
    void setRandomCentroids() {
        boolean tooNear;
        for (int i = 0; i < numClusters; i++) {
            tooNear = false;
            // Generate random position for initial centroid
            Point2DCentroid newInitPoint = new Point2DCentroid(rand.nextDouble() * MAX_WIDTH,
                    rand.nextDouble() * MAX_WIDTH, i);

            // Make sure no other centroid is too nearby
            for (Point2D point : initPoints) {
                if (newInitPoint.distanceFrom(point) < minDistance) {
                    i--;
                    tooNear = true;
                    break;
                }
            }

            // Add to list of initial centroids
            if (!tooNear) {
                initPoints.add(newInitPoint);
                System.out.println("Set initial centroid at " + newInitPoint);
            }
        }
    }


    /**
     * Generate a series of random Point2Ds with Gaussian distribution around clusters
     *
     * @return List of DataPoints
     */
    @Override
    public List<DataPoint> generatePoints() {
        System.out.println("Generating random Point2D data clusters..");
        List<DataPoint> randomPoints = new ArrayList<DataPoint>(numPoints);

        // Set initial random centroids
        setRandomCentroids();

        // Generate random points surrounding initial centroids
        for (Point2D cent : initPoints) {
            double variance = rand.nextDouble() * MAX_VARIANCE;
            for (int i = 0; i < Math.ceil(((double) numPoints) / numClusters) && randomPoints.size() < numPoints; i++) {
                double nextX = cent.getX() + (rand.nextGaussian() * variance);
                double nextY = cent.getY() + (rand.nextGaussian() * variance);
                randomPoints.add(new Point2D(nextX, nextY));
//                System.out.println("Added " + cent.getCluster() + " (" + nextX + ", " + nextY + ")");
            }
        }

        System.out.println("Generated " + randomPoints.size() + " random points in " + numClusters + " clusters!\n");
        return randomPoints;
    }
}
