package distsys;

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

        while(!done) {
            // Decide which test program you want to run
            System.out.println("\nWhich k-means clustering program do you want to run?");
            System.out.println("1. 2D Points (sequential)");
            System.out.println("2. 2D Points (parallel OpenMPI)");
            System.out.println("3. DNA Strings (sequential)");
            System.out.println("4. DNA Strings (parallel OpenMPI)");

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

            switch (program) {
                case 1:
                    System.out.println("2D Points (sequential)!");
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
    }
}
