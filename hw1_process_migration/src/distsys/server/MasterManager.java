package distsys.server;

import distsys.process.MigratableProcess;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/9/13
 */
public class MasterManager extends Thread {
    private static final int PING_TIMEOUT = 2500;
    private ServerSocket sock;

    private Timer pingTimer;

    private Map<Integer, Socket> liveSockets;           // A map of each slave ID to its Socket
    private List<Integer> liveSlaveIds;                 // A list of which slave IDs are still alive
    private Map<Integer, List<String>> activeProcesses; // Maps slave ID to a list of the processes it has

    private int totalProcesses = 0;         // Total number of processes created so far
    private int nextSlave = 0;              // ID for the next slave to join
    private int nextIdIndex = 0;             // Next index in liveSlaveIds; corresponds to a slave ID to send process to
    private boolean listening;              // Still listening for new slaves?


    /**
     * Initialize new Socket connection with Master
     * @param port int
     * @throws java.io.IOException
     */
    public MasterManager(int port) throws IOException {
        // Set up socket and port readers to master node
        sock = new ServerSocket(port);
        liveSockets = new HashMap<Integer, Socket>();
        liveSlaveIds = new ArrayList<Integer>();
        activeProcesses = new HashMap<Integer, List<String>>();

        // Initialize rebalance timer
        //TODO: INCORPORATE PING INTO LOAD BALANCING


        System.out.println("Master is listening for liveSockets at port " + port + "!");
        listening = true;
    }

    /**
     * Listen for new slave nodes to contact master
     */
    public void listen() {
        try {
            Socket newConnection = sock.accept();
            liveSockets.put(nextSlave, newConnection);
            liveSlaveIds.add(nextSlave);
            System.out.println("Added new connection from " + newConnection.getInetAddress().getCanonicalHostName() +
                    " (pid " + nextSlave + ")!");
            nextSlave++;
            nextIdIndex = liveSlaveIds.size()-1;
        } catch (IOException e) {
            if(listening) {
                System.err.println("Error: could not accept connection at local port " + sock.getLocalPort());
            }
        }
    }

    /**
     * Add a new process to be run by one of the available slaves
     * @param processClass String
     */
    public void addProcess(String processClass, String[] args) {
        MigratableProcess newProcess;
        try {
            // Use Reflection to pull out the class from its canonical name; instantiate it as newProcess
            Class<?> pClass = Class.forName(processClass);
            Constructor pConstructor = pClass.getConstructor(String[].class);
            newProcess = (MigratableProcess)pConstructor.newInstance((Object)args);
        } catch (ClassNotFoundException e) {
            System.err.println("Error: could not open class " + processClass + " (" + e.getMessage() + ").");
            return;
        } catch (NoSuchMethodException e) {
            System.err.println("Error: no constructor exists for " + processClass + " (" + e.getMessage() + ").");
            return;
        } catch (InstantiationException e) {
            System.err.println("Error: MigratableProcess instantiated wrong for " + processClass + " (" + e.getMessage() + ").");
            return;
        } catch (IllegalAccessException e) {
            System.err.println("Error: Illegal access for " + processClass + " (" + e.getMessage() + ")?");
            return;
        } catch (InvocationTargetException e) {
            System.err.println("Error: Invocation error for " + processClass + " (" + e.getMessage() + ")?");
            return;
        }
        // Made it all the way through successfully through the maze of exceptions

        // Create a new process name
        String processName = totalProcesses + "-" + newProcess.toString();
        newProcess.setProcessName(processName);

        // Prepare the process to be serialized and sent away
        sendProcess(newProcess, liveSlaveIds.get(nextIdIndex));

        // Add process to map of 'slaves -> list<processes>'
        nextIdIndex = (nextIdIndex + 1) % liveSlaveIds.size();
        totalProcesses++;
    }

    /**
     * Serialize newProcess and send it to the next slave machine
     * @param newProcess    MigratableProcess
     * @param slaveId       int
     */
    private void sendProcess(MigratableProcess newProcess, int slaveId) {
        // Create new ServerMessage indicating for the receiving slave to run a new process
        ServerMessage newProcessMsg = new ServerMessage(ServerMessage.MessageType.RUN, newProcess);
        Socket currentSocket = liveSockets.get(slaveId);
        try {
            ObjectOutputStream sockOut = new ObjectOutputStream(currentSocket.getOutputStream());
            sockOut.writeObject(newProcessMsg);
            sockOut.flush();
            System.out.println("Sent msg " + newProcessMsg + " to slave " + slaveId);
        } catch (IOException e) {
            System.err.println("Error: error serializing msg " + newProcessMsg + "(" + e.getMessage() + ").");
        }
    }


    /**
     * Move a MigratableProcess from one slave to another
     * @param processName String
     * @param fromSlaveId int
     * @param toSlaveId   int
     */
    public void migrateProcess(String processName, int fromSlaveId, int toSlaveId) {
        // Create ServerMessage to sell slave to suspend a process
        Socket fromSock = liveSockets.get(fromSlaveId);
        ServerMessage suspendProcessMsg = new ServerMessage(ServerMessage.MessageType.SUSPEND, processName);
        ServerMessage recSuspendProcessMsg;

        try {
            // Send message to fromSlave socket telling it suspend the given process and send it back here
            ObjectOutputStream fromSockOut = new ObjectOutputStream(fromSock.getOutputStream());
            fromSockOut.writeObject(suspendProcessMsg);
            fromSockOut.flush();
            System.out.println("Sent msg " + suspendProcessMsg + " to slave " + fromSlaveId);

            // Wait for a ServerMessage response, bail if it isn't valid
            ObjectInputStream fromSockIn = new ObjectInputStream(fromSock.getInputStream());
            recSuspendProcessMsg = (ServerMessage)fromSockIn.readObject();
            if(!(recSuspendProcessMsg.getType() == ServerMessage.MessageType.SUSPEND &&
                    recSuspendProcessMsg.getPayload() != null)) {
                System.err.println("Error: slave " + fromSlaveId + " did not send back suspended process " + processName);
                return;
            }

            // Send this suspended and serialized process off to a new home
            sendProcess((MigratableProcess)recSuspendProcessMsg.getPayload(), toSlaveId);
        } catch (IOException e) {
            System.err.println("Error: could not migrate process " + processName + " properly. (" + e.getMessage() + ").");
        } catch (ClassNotFoundException e) {
            System.err.println("Error: Master received an object that wasn't a ServerMessage (" + e.getMessage() + ")");
        }
    }

    /**
     * Migrate processes around in order to have equal number in each slave
     */
    public void balanceProcessLoad() {

    }


    /**
     * Print the names of all active processes and where they live
     */
    public void listProcesses() {
        // Ping all slaves first to make sure list is up-to-date
        pingSlaves();

        // If no current processes, don't bother iterating
        if(liveSlaveIds.size() == 0) {
            System.out.println("\n-- No Active Processes --");
            return;
        }

        // Iterate through all noted active clients and print its processes
        System.out.println("\n-- Active Processes --");
        for(Integer i : liveSlaveIds) {
            System.out.println("Slave Client " + i);
            for(String process : activeProcesses.get(i)) {
                System.out.println("  " + process);
            }
        }
    }

    /**
     * Ping every slave to see which ones are alive still
     * Note: there is generic casting from Object to Set towards the bottom; it's probably fine, it's straight from Mr. Slave
     */
    @SuppressWarnings("unchecked")
    public void pingSlaves() {
        List<Integer> deadSlaveIds = new ArrayList<Integer>();
        String statusOut = "Ping: Processes alive [";

        // Iterate through living slaves
        for(Integer i : liveSlaveIds) {
            Socket s = liveSockets.get(i);
            ServerMessage sentMessage = new ServerMessage(ServerMessage.MessageType.PING, i);
            ServerMessage recMessage;

            try {
                // Send a message to the slave's socket and wait to see if it responds
                s.setSoTimeout(PING_TIMEOUT);
                ObjectOutputStream sockOut = new ObjectOutputStream(s.getOutputStream());
                sockOut.writeObject(sentMessage);
                sockOut.flush();

                // Wait for response, if valid, continue to next one
                ObjectInputStream sockIn = new ObjectInputStream(s.getInputStream());
                recMessage = (ServerMessage)sockIn.readObject();
                if(recMessage.getType() == ServerMessage.MessageType.ALIVE) {
                    statusOut += " " + i;
                    // Slave is alive, make note of its processes
                    activeProcesses.put(i, (ArrayList<String>)recMessage.getPayload());
                }
            } catch (IOException e) {
                // Slave didn't respond, presumed dead
                System.out.println("Slave " + i + " did not respond in time, presumed lost.");
                deadSlaveIds.add(i);
            } catch (ClassNotFoundException e) {
                System.err.println("Error: Master received an object that wasn't a ServerMessage (" + e.getMessage() + ")");
            }
        }
        System.out.println(statusOut + " ]");

        // Remove all dead slaves from the lists of the living ones (RIP)
        removeDeadSlaves(deadSlaveIds);
    }

    /**
     * Removes any IDs and Sockets associated with dead slaves
     * @param deadSlaveIds List<Integer>
     */
    private void removeDeadSlaves(List<Integer> deadSlaveIds) {
        for(Integer i : deadSlaveIds) {
            int idPosition = liveSlaveIds.indexOf(i);
            if(idPosition >= 0) {
                liveSlaveIds.remove(idPosition);
            }
            liveSockets.remove(i);
            activeProcesses.remove(i);
        }
    }


    /**
     * Close master socket and slaves attached to it
     */
    public void close() {
        listening = false;
        try {
            sock.close();
            for(Integer i : liveSlaveIds) {
                Socket s = liveSockets.get(i);
                ServerMessage message = new ServerMessage(ServerMessage.MessageType.QUIT, null);
                try {
                    ObjectOutputStream sockOut = new ObjectOutputStream(s.getOutputStream());
                    sockOut.writeObject(message);
                    sockOut.flush();
                    System.out.println("Sent msg " + message + " to slave " + i);
                } catch (IOException e) {
                    System.err.println("Error: error serializing msg " + message + "(" + e.getMessage() + ").");
                }
            }
        } catch(IOException e) {
            System.err.println("Error: problem closing master socket ports.\n" + e.getMessage());
        }
    }

    @Override
    public void run() {
        while(listening) {
            listen();
        }
    }

}