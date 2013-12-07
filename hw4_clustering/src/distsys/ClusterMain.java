package distsys;

import distsys.datagen.*;
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
public class ClusterMain {

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


    public static void main(String[] args) {
        System.out.println("Project 4 - Sequential and Parallel Clustering!\n");

        int program;
        int numPoints;
        int numClusters;
        int strandLength = 10;
        String hostFile;
        boolean done = false;
        UserInput inputReader = new UserInput();

        while (!done) {
//            try {     TODO: uncomment
//                // Read user input on all the program parameters
//                program = inputReader.getProgramNum();
//                numPoints = inputReader.getNumDataPoints();
//                numClusters = inputReader.getNumClusters();
//                if(program == 3 || program == 4) {
//                    strandLength = inputReader.getStrandLength();
//                }
//            } catch (NumberFormatException e) {
//                continue;
//            }
//
//            // Don't be a jerk, use fewer clusters
//            if(numClusters >= numPoints) {
//                System.out.println("You should probably look for fewer clusters than there are points. " +
//                        "Pigeon-hole principle and everything, you know.");
//                continue;
//            }


            //TODO remove comments above!
            program = 3;
            numPoints = 100;
            numClusters = 5;
            strandLength = 30;

            WallClock clock = new WallClock();
            KMeansSequential kmeans;
            DataGenerator dataGen;
            List<DataPoint> dataPoints = null;

            // Choose which type of algorithm to run
            switch (program) {
                case 1:
                    System.out.println("Running K-Means clustering (sequential) on 2D points..\n");
                    dataGen = new Point2DGenerator(numPoints, numClusters);
                    break;
//                case 2:
//                    System.out.println("2D Points (parallel)!");
//                    break;
                case 3:
                    System.out.println("Running K-Means clustering (sequential) on DNA strands..\n");
                    dataGen = new DnaStrandGenerator(numPoints, numClusters, strandLength);
                    break;
//                case 4:
//                    System.out.println("DNA (parallel)!!");
//                    break;
                default:
                    System.out.println("Unknown program number " + program + "??");
                    return;
            }

            // Run (sequential?) k-means clustering
            // Generate data points
            dataPoints = dataGen.generatePoints();

            // Run K-Means clustering
            clock.startTimer();
            kmeans = new KMeansSequential(numPoints, numClusters);
            kmeans.setDataPoints(dataPoints);
            kmeans.findClusters();

            long stopTime = clock.getRunTime();
            System.out.printf("\nClustering completed in %.4f seconds!\n", stopTime / 1000.0);

            // Print clustered results to a file
            if (dataPoints != null) {
                writeResultsToFile(kmeans.getDataPoints(), numClusters);
            }

            done = true;
        }

//        lineIn.nextLine();  //todo remove
    }
}
