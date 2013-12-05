package distsys;

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
import java.util.Scanner;

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

        Scanner lineIn = new Scanner(System.in);
        int program;
        int numPoints;
        int numClusters;
        String hostFile;
        boolean done = false;

        while (!done) {
            // Decide which test program you want to run
            System.out.println("\nWhich k-means clustering program do you want to run?");
            System.out.println("1. 2D Points (sequential)");
            System.out.println("2. 2D Points (parallel OpenMPI)");
            System.out.println("3. DNA Strings (sequential)");
            System.out.println("4. DNA Strings (parallel OpenMPI)");

            /*
            String input = lineIn.nextLine();
            try {
                program = Integer.parseInt(input);
                if(program < 1 || program > 4) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid response, please enter one of the proper numeric choices.");
                continue;
            }

            // Number of datapoints
            System.out.println("\nHow many data points should we use?");
            input = lineIn.nextLine();
            try {
                numPoints = Integer.parseInt(input);
                if(numPoints <= 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid response, please enter a positive number of points.");
                continue;
            }

            // Number of clusters
            System.out.println("\nHow many clusters should we find?");
            input = lineIn.nextLine();
            try {
                numClusters = Integer.parseInt(input);
                if(numClusters <= 0) {
                    throw new NumberFormatException();
                }
                System.out.println();
            } catch (NumberFormatException e) {
                System.out.println("Invalid response, please enter a positive number of clusters.");
                continue;
            }

            // Don't be a jerk, use fewer clusters
            if(numClusters >= numPoints) {
                System.out.println("You should probably look for fewer clusters than there are points. " +
                        "Pigeon-hole principle and everything, you know.");
                continue;
            }
            */
            //TODO remove comments above!
            program = 3;
            numPoints = 9;
            numClusters = 2;

            WallClock clock = new WallClock();
            List<DataPoint> dataPoints = null;
            switch (program) {
                case 1:
                    System.out.println("Running K-Means clustering (sequential) on 2D points..\n");
                    // Generate data points
                    Point2DGenerator pointGen = new Point2DGenerator(numPoints, numClusters);
                    dataPoints = pointGen.generatePoints();

                    // Run K-Means clustering
                    clock.startTimer();
                    KMeansSequential kmeans = new KMeansSequential(numPoints, numClusters);
                    kmeans.setDataPoints(dataPoints);
                    kmeans.findClusters();

                    long stopTime = clock.getRunTime();
                    System.out.printf("\nClustering completed in %.4f seconds!\n", stopTime / 1000.0);
                    break;
                case 2:
                    System.out.println("2D Points (parallel)!");
                    break;
                case 3:
                    System.out.println("Running K-Means clustering (sequential) on DNA strands..\n");
                    // Generate data points
                    DnaStrandGenerator dnaStrandGen = new DnaStrandGenerator(numPoints, numClusters, 10);   //TODO ask for length
                    dataPoints = dnaStrandGen.generatePoints();

                    // Run K-Means clustering
                    clock.startTimer();
                    kmeans = new KMeansSequential(numPoints, numClusters);
                    kmeans.setDataPoints(dataPoints);
                    kmeans.findClusters();

                    stopTime = clock.getRunTime();
                    System.out.printf("\nClustering completed in %.4f seconds!\n", stopTime / 1000.0);
                    break;
                case 4:
                    System.out.println("DNA (parallel)!!");
                    break;
                default:
                    System.out.println("Unknown program number " + program + "??");
                    return;
            }

            // Print clustered results to a file
            if (dataPoints != null) {
                writeResultsToFile(dataPoints, numClusters);
            }

            done = true;
        }

//        lineIn.nextLine();  //todo remove
    }
}
