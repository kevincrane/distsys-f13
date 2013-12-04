package distsys;

import distsys.datagen.Point2DGenerator;
import distsys.datagen.WallClock;
import distsys.kmeans.DataPoint;
import distsys.kmeans.KMeansSequential;

import java.util.List;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/2/13
 */
public class ClusterMain {
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
            program = 1;
            numPoints = 1000000;
            numClusters = 3;

            WallClock clock = new WallClock();
            switch (program) {
                case 1:
                    System.out.println("Running K-Means clustering (sequential) on 2D points..\n");
                    // Generate data points
                    Point2DGenerator pointGen = new Point2DGenerator(numPoints, numClusters);
                    List<DataPoint> dataPoints = pointGen.generatePoints();

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
                    System.out.println("DNA (sequential)!");
                    break;
                case 4:
                    System.out.println("DNA (parallel)!!");
                    break;
                default:
                    System.out.println("Unknown program number " + program + "??");
                    break;
            }

            done = true;
        }

        lineIn.nextLine();  //todo remove
    }
}
