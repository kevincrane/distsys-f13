package distsys;

import distsys.server.MasterManager;
import distsys.server.SlaveManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 9/6/13
 */
public class ProcessManager {

    private static final int MASTER_PORT = 23456;


    /**
     * Main method starts here
     * @param args    String[]
     */
    public static void main(String[] args) {
        if(args.length == 2 && args[0].equals("-c")) {
            // Starting Slave ProcessManager on client machine
            String hostname = args[1];
            try {
                System.out.println("Running new slave node.");
                SlaveManager slave = new SlaveManager(hostname, MASTER_PORT);
                slave.listen();
                slave.close();
            } catch(IOException e) {
                System.err.println("Could not connect to socket at " + hostname + ":" + MASTER_PORT + " (" +
                        e.getMessage() + ").");
                e.printStackTrace();
            }
        } else if(args.length == 0) {
            // Starting Master ProcessManager on server machine
            try {
                System.out.println("Running new master node.");
                MasterManager master = new MasterManager(MASTER_PORT);
                master.start();

                // Receive user input for a while
                runManager(master);
            } catch(IOException e) {
                System.err.println("Master could not listen at port " + MASTER_PORT + ".\n" + e.getMessage());
            }
        } else {
            System.out.println("usage: java ProcessManager [-c <master hostname>]");
        }
    }

    /**
     * Receive user input and commands for MasterManager
     */
    private static void runManager(MasterManager manager) {
        boolean running = true;
        Scanner lineIn = new Scanner(System.in);
        int command;
        while(running) {
            System.out.println("\nEnter commands for the Master server:");
            System.out.println("1. Run new process | 2. Process Status | 3. Quit");
            String input = lineIn.nextLine();

            try {
                command = Integer.parseInt(input);
            } catch(NumberFormatException e) {
                System.out.println("Invalid response, please enter one of the proper numeric choices.");
                continue;
            }

            // Send command to master
            switch(command) {
                case 1:
                    System.out.println("Please enter the fully-qualified classname and arguments of the process to run:");
                    System.out.println("    e.g. 'distsys.process.CountProcess 25'");
                    input = lineIn.nextLine();
                    String[] tokens = input.split(" ");
                    String className = tokens[0];

                    if(tokens.length == 1) {
                        manager.addProcess(className, new String[0]);
                    } else {
                        String[] args = Arrays.copyOfRange(tokens, 1, tokens.length);
                        manager.addProcess(className, args);
                    }
                    break;
                case 2:
                    manager.listProcesses();
                    break;
                case 3:
                    manager.close();
                    running = false;
                    System.out.println("\nByeee!!");
                    break;
                default:
                    System.out.println("Invalid response, please enter one of the proper numeric choices.");
                    break;
            }
        }
    }

}
