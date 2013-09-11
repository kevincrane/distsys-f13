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
    private Socket sock;
    private ObjectInputStream sockIn;
    private ObjectOutputStream sockOut;


    /**
     * Initialize new Socket connection with Master
     * @param hostname
     * @param port
     * @throws IOException
     */
    public SlaveManager(String hostname, int port) throws IOException {
        // Set up socket and port readers to master node
        sock = new Socket(hostname, port);
        processList = new HashMap<String, MigratableProcess>();
        System.out.println("Connected to socket at " + hostname + ":" + port + "!");
    }

    /**
     * Listen for messages from Master and act on them
     */
    public void listen() throws IOException {
        ServerMessage newMessage;
        while(sock.isConnected()) {
            // Listen for incoming ServerMessage
            sockIn = new ObjectInputStream(sock.getInputStream());

            // Read ServerMessage, take appropriate action
            try {
                newMessage = (ServerMessage)sockIn.readObject();
            } catch (ClassNotFoundException e) {
                System.err.println("Error: Slave received an object that wasn't a ServerMessage (" +
                        e.getMessage() + ")");
                continue;
            }
            switch (newMessage.getType()) {
                case RUN:
                    receiveProcess((MigratableProcess)newMessage.getPayload());
                    break;
                case PING:
                    sockOut = new ObjectOutputStream(sock.getOutputStream());

                    // Create response to server with list of all process names
                    ServerMessage pingResponse = new ServerMessage(ServerMessage.MessageType.ALIVE, processKeySetToList());
                    sockOut.writeObject(pingResponse);
                    sockOut.flush();
                    //TODO: somewhere, check if threads still alive; on timer maybe?
                    break;
                case QUIT:
                    close();
                    System.out.println("\nClosing down SlaveManager at Master's request!");
                    break;
                default:
                    System.err.println("Error: Unknown ServerMessage type " + newMessage.getType() + ".");
                    break;
            }
        }
    }

    /**
     * Receive a serialized process from the socket;
     * Deserialize and run it
     * @param newProcess
     */
    private void receiveProcess(MigratableProcess newProcess) {
        // Add process to new list of processes
        processList.put(newProcess.getProcessName(), newProcess);
        Thread processThread = new Thread(newProcess);

        // Run the given process
        processThread.start();
    }

    /**
     * Iterate through each process belonging to the slave and delete it from the name list if it's completed
     */
    private void checkProcessesLiveness() {

    }

    /**
     * Close the socket ports and writers
     */
    public void close() {
        try {
            sockOut.close();
            sockIn.close();
            sock.close();
        } catch(IOException e) {
            System.err.println("Error: problem closing slave socket ports.\n" + e.getMessage());
        }
    }

// ##### HELPERS #####
    private ArrayList<String> processKeySetToList() {
        ArrayList<String> processNameList = new ArrayList<String>();
        for(String processName : processList.keySet()) {
            processNameList.add(processName);
        }
        return processNameList;
    }

}
