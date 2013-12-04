package distsys.datagen;

import distsys.kmeans.DataPoint;

import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/4/13
 */
abstract class DataGenerator {

    final int numPoints;
    final int numClusters;
    final Random rand;

    /**
     * Used to generate random data in approximate clusters to be identified with k-means
     *
     * @param numPoints   Number of random points to generate
     * @param numClusters Number of clusters to generate
     */
    DataGenerator(int numPoints, int numClusters) {
        this.numPoints = numPoints;
        this.numClusters = numClusters;
        this.rand = new Random();
    }

    /**
     * Generate initial random centroid starting spots and set to List<DataPoint>
     */
    abstract void setRandomCentroids();

    /**
     * Generate a series of random points in clusters
     *
     * @return List of DataPoints
     */
    public abstract List<DataPoint> generatePoints();
}
