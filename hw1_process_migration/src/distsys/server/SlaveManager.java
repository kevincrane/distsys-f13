package distsys.server;

import distsys.process.MigratableProcess;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/9/13
 */
public class SlaveManager {
    private Map<String, MigratableProcess> processList;
    private Map<String, Thread> threadList;
    private Socket sock;
    private Timer liveTimer;
    private ObjectInputStream sockIn;
    private ObjectOutputStream sockOut;


    /**
     * Initialize new Socket connection with Master
     *
     * @param hostname String
     * @param port     int
     * @throws IOException
     */
    public SlaveManager(String hostname, int port) throws IOException {
        // Set up socket and port readers to master node
        sock = new Socket(hostname, port);
        processList = new HashMap<String, MigratableProcess>();
        threadList = new HashMap<String, Thread>();

        // Create Timer to check liveness of all processes currently running here
        liveTimer = new Timer();
        liveTimer.schedule(new TimerTask() {
            public void run() {
                checkProcessesLiveness();
            }
        }, 500, 500);

        System.out.println("Connected to socket at " + hostname + ":" + port + "!");
    }

    /**
     * Listen for messages from Master and act on them
     */
    public void listen() throws IOException {
        ServerMessage newMessage;
        while (sock.isConnected()) {
            // Listen for incoming ServerMessage
            sockIn = new ObjectInputStream(sock.getInputStream());

            // Read ServerMessage, take appropriate action
            try {
                newMessage = (ServerMessage) sockIn.readObject();
            } catch (ClassNotFoundException e) {
                System.err.println("Error: Slave received an object that wasn't a ServerMessage (" +
                        e.getMessage() + ")");
                continue;
            }

            // Perform correct action based on message type
            switch (newMessage.getType()) {
                case RUN:
                    // Received a new MigratableProcess to run
                    receiveProcess((MigratableProcess) newMessage.getPayload());
                    break;
                case PING:
                    // Master wants to check in on you; Create response to server with list of all process names
                    sockOut = new ObjectOutputStream(sock.getOutputStream());
                    ServerMessage pingResponse = new ServerMessage(ServerMessage.MessageType.ALIVE, processKeySetToList());
                    sockOut.writeObject(pingResponse);
                    sockOut.flush();
                    break;
                case SUSPEND:
                    // Suspend the process and send it back to the Master
                    suspendProcess((String) newMessage.getPayload());
                    break;
                case QUIT:
                    // Time to close up shop
                    close();
                    System.out.println("\nClosing down SlaveManager at Master's request!");
                    return;
                default:
                    // Oops :(
                    System.err.println("Error: Unknown ServerMessage type " + newMessage.getType() + ".");
                    break;
            }
        }
    }


    /**
     * Receive a serialized process from the socket;
     * Deserialize and run it
     *
     * @param newProcess MigratableProcess
     */
    private void receiveProcess(MigratableProcess newProcess) {
        // Add process to new list of processes
        Thread processThread = new Thread(newProcess);
        processList.put(newProcess.getProcessName(), newProcess);
        threadList.put(newProcess.getProcessName(), processThread);

        // Run the given process
        processThread.start();
    }

    /**
     * Suspend and serialize a process and send it back to the Master
     *
     * @param processName String
     */
    private void suspendProcess(String processName) {
        // Suspend process
        MigratableProcess suspendProcess = processList.get(processName);
        if (suspendProcess == null) {
            System.err.println("Error: Process " + processName + " does not exist on this slave.");
            suspendProcess = null;
        } else {
            suspendProcess.suspend();
        }

        try {
            // Serialize suspended process and send back to Master
            sockOut = new ObjectOutputStream(sock.getOutputStream());
            ServerMessage suspendedResponse = new ServerMessage(ServerMessage.MessageType.SUSPEND, suspendProcess);
            sockOut.writeObject(suspendedResponse);
            sockOut.flush();
        } catch (IOException e) {
            System.err.println("Error: could not send process " + processName + " to Master properly. (" + e.getMessage() + ").");
            return;
        }

        // Remove process from list of active processes
        processList.remove(processName);
        threadList.remove(processName);
    }

    /**
     * Iterate through each process belonging to the slave and delete it from the name list if it's completed
     */
    private void checkProcessesLiveness() {
        // Find all threads that aren't alive
        List<String> completedProcesses = new ArrayList<String>();
        for (String s : threadList.keySet()) {
            if (!threadList.get(s).isAlive()) {
                completedProcesses.add(s);
            }
        }

        // Delete these processes from the two process/thread lists
        for (String s : completedProcesses) {
            threadList.remove(s);
            processList.remove(s);
        }
    }

    /**
     * Close the socket ports and writers
     */
    public void close() {
        try {
            // Try to let other processes finish up first
            for (String s : threadList.keySet()) {
                if (threadList.get(s).isAlive()) {
                    try {
                        threadList.get(s).join(2500);
                    } catch (InterruptedException e) {
                        // Do nothing
                    }
                }
            }

            // Close the ports down
            liveTimer.cancel();
            sockOut.close();
            sockIn.close();
            sock.close();
        } catch (IOException e) {
            System.err.println("Error: problem closing slave socket ports.\n" + e.getMessage());
        }
    }

    // ##### HELPERS #####
    private ArrayList<String> processKeySetToList() {
        ArrayList<String> processNameList = new ArrayList<String>();
        for (String processName : processList.keySet()) {
            processNameList.add(processName);
        }
        return processNameList;
    }

}
