package distsys;

import distsys.datagen.DataGenerator;
import distsys.datagen.DnaStrandGenerator;
import distsys.datagen.Point2DGenerator;
import distsys.datagen.WallClock;
import distsys.kmeans.DataPoint;
import distsys.kmeans.KMeansSequential;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/2/13
 */
public class ClusterMainSeq {

    private static final String outputFile = "clusteredData.txt";

    /**
     * Write clustered results to a file, sorted by cluster
     *
     * @param dataPoints List of DataPoints that have been clustered
     */
    public static void writeResultsToFile(List<DataPoint> dataPoints, int numClusters) {
        List<List<DataPoint>> clusteredPoints = new ArrayList<List<DataPoint>>(numClusters);
        for (int i = 0; i < numClusters; i++) {
            // Initialize each cluster of points
            clusteredPoints.add(new ArrayList<DataPoint>());
        }

        // Add each DataPoint to its corresponding cluster
        for (DataPoint point : dataPoints) {
            int pointCluster = point.getCluster();
            if (pointCluster >= 0 && pointCluster < numClusters) {
                clusteredPoints.get(pointCluster).add(point);
            }
        }

        try {
            // Write the points of each cluster to the output file
            Writer outFile = new FileWriter(outputFile);
            for (List<DataPoint> cluster : clusteredPoints) {
                for (DataPoint point : cluster) {
                    outFile.write(point + "\n");
                }
                outFile.write("\n");
            }
            outFile.close();
        } catch (IOException e) {
            System.err.println("Error: wasn't able to write output to file " + outputFile + " (" + e.getMessage() + ").");
        }
    }

    /**
     * Command line usage info
     */
    public static void usage() {
        System.err.println("Usage: ClusterMainSeq {points|dna} [data_count] [cluster_count] (strand_length)");
    }


    public static void main(String[] args) {
        System.out.println("Project 4 - Sequential and Parallel Clustering!");
        int numPoints;
        int numClusters;
        int strandLength = 10;

        if (args.length != 3 && args.length != 4) {
            usage();
            return;
        }

        try {
            // Read command line input
            numPoints = Integer.parseInt(args[1]);
            numClusters = Integer.parseInt(args[2]);
            if (args.length == 4) {
                strandLength = Integer.parseInt(args[3]);
            }

            if (numPoints <= 0 || numClusters <= 0 || strandLength <= 0) {
                System.err.println("Error: all numeric arguments must be positive.");
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            usage();
            return;
        }

        // Don't be a jerk, use fewer clusters
        if (numClusters >= numPoints) {
            System.out.println("You should probably look for fewer clusters than there are points. " +
                    "Pigeon-hole principle and everything, you know.");
            usage();
            return;
        }


        // Objects needed to perform clustering
        KMeansSequential kmeans = new KMeansSequential(numPoints, numClusters);
        DataGenerator dataGen;
        List<DataPoint> dataPoints;
        WallClock clock = new WallClock();

        // Select which type of data to generate
        if (args[0].equalsIgnoreCase("points")) {
            dataGen = new Point2DGenerator(numPoints, numClusters);
            System.out.println("Running K-Means clustering (sequential) on 2D points..\n");
        } else if (args.length == 4 && args[0].equalsIgnoreCase("dna")) {
            dataGen = new DnaStrandGenerator(numPoints, numClusters, strandLength);
            System.out.println("Running K-Means clustering (sequential) on DNA strands..\n");
        } else {
            System.err.println("Data name should only 'points' or 'dna'.");
            usage();
            return;
        }

        // Time to run k-means clustering!!
        // Generate data points
        dataPoints = dataGen.generatePoints();
        kmeans.setDataPoints(dataPoints);

        // Fetch the clusters!
        clock.startTimer();
        kmeans.findClusters();

        // Stop the timer and print clustered results to a file
        if (kmeans.getDataPoints() != null) {
            long stopTime = clock.getRunTime();
            System.out.printf("\nClustering completed in %.4f seconds!\n\n", stopTime / 1000.0);
            writeResultsToFile(kmeans.getDataPoints(), numClusters);
            System.out.println("Output written to " + outputFile + ".");
        }
    }
}
