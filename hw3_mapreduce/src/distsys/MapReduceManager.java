package distsys;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class MapReduceManager {

    static MasterNode masterNode;

    public static void kdfsOps(Scanner lineIn) {
        System.out.println("1. Read File | 2. Store New File | 3. List All Files | 4. Quit");
        String input = lineIn.nextLine();
        int command;

        try {
            command = Integer.parseInt(input);
        } catch (NumberFormatException e) {
            System.out.println("Invalid response, please enter one of the proper numeric choices.");
            return;
        }

        // Send command to master
        switch (command) {
            case 1:
                // Read KDFS file
                System.out.println("Enter the name of the file on KDFS to read:");
                input = lineIn.nextLine();
                System.out.println();
                System.out.println(masterNode.readFile(input));
                break;
            case 2:
                // Store new KDFS file
                System.out.println("Enter the name of the file to store on KDFS:");
                input = lineIn.nextLine();
                System.out.println();
                masterNode.putFile(input);
            case 3:
                masterNode.listFiles();
                break;
            default:
                System.out.println("Invalid response, please enter one of the proper numeric choices.");
                break;
        }
    }

    /**
     * Main method
     */
    public static void main(String[] args) throws IOException {
        System.out.println("Welcome to CMU MapReduce!\n");
        System.out.println("Starting MasterNode thread..");

        // Start MasterNode thread
        masterNode = new MasterNode();
        masterNode.start();

        // Run user input loop
        boolean running = true;
        Scanner lineIn = new Scanner(System.in);
        int command;
        while (running) {
            System.out.println("\nEnter commands for the MapReduce master server:");
            System.out.println("1. Run new MapReduce job | 2. KDFS ops | 3. Quit");
            String input = lineIn.nextLine();

            try {
                command = Integer.parseInt(input);
            } catch (NumberFormatException e) {
                System.out.println("Invalid response, please enter one of the proper numeric choices.");
                continue;
            }

            // Send command to master
            switch (command) {
                case 1:
                    //TODO map reduce stuff (start job, list jobs)
                    break;
                case 2:
                    kdfsOps(lineIn);
                    break;
                case 3:
                    // Quit
                    //TODO kill masterNode somehow
                    running = false;
                    System.out.println("\nGoodBye!!");
                    break;
                default:
                    System.out.println("Invalid response, please enter one of the proper numeric choices.");
                    break;
            }
        }
    }

}