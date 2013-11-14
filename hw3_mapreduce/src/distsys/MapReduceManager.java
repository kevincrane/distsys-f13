package distsys;

import distsys.mapreduce.MapReduceJob;
import distsys.mapreduce.Task;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class MapReduceManager {

    static MasterNode masterNode;

    /**
     * Take user input and launch a new Map Reduce job
     *
     * @param lineIn Scanner to read user input from
     */
    public static void newMapReduceJob(Scanner lineIn) {
        System.out.println("Enter the class, input file, and output file of the map reduce job you want to run:");
        System.out.println("  e.g. distsys.examples.WordCountMRJob inputFile.txt output.txt");
        String input = lineIn.nextLine();

        // Read MapReduce parameters from input
        String[] params = input.split(" ");
        if (params.length != 3) {
            System.out.println("Invalid input. Format: ClassName InputFile OutputFile");
            return;
        }

        try {
            // Try to instantiate MapReduceJob from user input
            MapReduceJob newJob = (MapReduceJob) Class.forName(params[0]).newInstance();
            newJob.setInputFile(params[1]);
            newJob.setOutputFile(params[2]);

            // Send job to MasterNode to begin queuing up
            masterNode.newMapReduceJob(newJob);
            System.out.println("Sent a new " + params[0] + " map reduce job to the MasterNode!");
        } catch (Exception e) {
            // Yes, it's bad practice to catch generic exceptions, but it's cleaner to read and just as practical here
            System.out.println("Error: could not open MapReduce class " + params[0]);
        }
    }

    /**
     * Take user input and perform a file system action using the KDFS
     *
     * @param lineIn Scanner to read user input from
     */
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
            System.out.println("1. Run new MapReduce job | 2. List MapReduce jobs | 3. KDFS ops | 4. Quit");
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
                    newMapReduceJob(lineIn);
//                    MapReduceJob mapReduceJob = new IdentityMRJob();
//                    mapReduceJob.setInputFile("alice.txt");
//                    mapReduceJob.setOutputFile("alice_output.txt");
//                    masterNode.newMapReduceJob(mapReduceJob);
                    break;
                case 2:
                    // List all active MapReduce jobs
                    System.out.println("\nListing MapReduce Jobs:");
                    HashMap<Integer, List<Task>> jobMap = masterNode.getRunningMapReduceJobs();
                    for (int slaveId : jobMap.keySet()) {
                        System.out.println("SLAVE " + slaveId + " is running:");
                        for (Task t : jobMap.get(slaveId)) {
                            System.out.println(t);
                        }
                        System.out.println();
                    }
                    break;
                case 3:
                    // KDFS Operations
                    kdfsOps(lineIn);
                    break;
                case 4:
                    // Quit
                    masterNode.close();
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