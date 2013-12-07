package distsys.datagen;

import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/5/13
 * <p/>
 * Note: No longer needed because we switched to using command-line arguments
 */
public class UserInput {

    Scanner lineIn;

    /**
     * Simple class to hide all the taking of user input for program parameters
     */
    public UserInput() {
        lineIn = new Scanner(System.in);
    }

    public int getProgramNum() throws NumberFormatException {
        // Decide which test program you want to run
        System.out.println("\nWhich k-means clustering program do you want to run?");
        System.out.println("1. 2D Points");
        System.out.println("2. DNA Strings");

        String input = lineIn.nextLine();
        int program;
        try {
            program = Integer.parseInt(input);
            if (program != 1 && program != 2) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid response, please enter one of the proper numeric choices.");
            throw e;
        }
        return program;
    }

    public int getNumDataPoints() throws NumberFormatException {
        // Number of datapoints
        int numPoints;
        System.out.println("\nHow many data points should we use?");
        String input = lineIn.nextLine();
        try {
            numPoints = Integer.parseInt(input);
            if (numPoints <= 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid response, please enter a positive number of points.");
            throw e;
        }
        return numPoints;
    }

    public int getNumClusters() throws NumberFormatException {
        // Number of clusters
        int numClusters;
        System.out.println("\nHow many clusters should we find?");
        String input = lineIn.nextLine();
        try {
            numClusters = Integer.parseInt(input);
            if (numClusters <= 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid response, please enter a positive number of clusters.");
            throw e;
        }
        return numClusters;
    }

    public int getStrandLength() throws NumberFormatException {
        // Length of DNA strand
        int strandLength;
        System.out.println("\nHow many nucleotides in each DNA strand?");
        String input = lineIn.nextLine();
        try {
            strandLength = Integer.parseInt(input);
            if (strandLength <= 0) {
                throw new NumberFormatException();
            }
            System.out.println();
        } catch (NumberFormatException e) {
            System.out.println("Invalid response, please enter a positive number of nucleotides.");
            throw e;
        }
        return strandLength;
    }
}
