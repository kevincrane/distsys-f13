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

    private Map<Integer, Socket> liveSockets;
    private List<Integer> liveSlaveIds;
    private List<String> processNames;

    private int nextSlave = 0;
    private int nextToSend = 0;
    private boolean listening;


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
        processNames = new ArrayList<String>();

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
        } catch (IOException e) {
            if(listening) {
                System.err.println("Error: could not accept connection at local port " + sock.getLocalPort());
            }
        }
    }

    /**
     * Add a new process to be run by one of the available slaves
     * @param processName String
     */
    public void addProcess(String processName, String[] args) {
        MigratableProcess newProcess;
        try {
            Class<?> pClass = Class.forName(processName);
            Constructor pConstructor = pClass.getConstructor(String[].class);
            newProcess = (MigratableProcess)pConstructor.newInstance((Object)args);
        } catch(ClassNotFoundException e) {
            System.err.println("Error: could not open class " + processName + " (" + e.getMessage() + ").");
            return;
        } catch(NoSuchMethodException e) {
            System.err.println("Error: no constructor exists for " + processName + " (" + e.getMessage() + ").");
            return;
        } catch(InstantiationException e) {
            System.err.println("Error: MigratableProcess instantiated wrong for " + processName + " (" + e.getMessage() + ").");
            return;
        } catch(IllegalAccessException e) {
            System.err.println("Error: Illegal access for " + processName + " (" + e.getMessage() + ")?");
            return;
        } catch (InvocationTargetException e) {
            System.err.println("Error: Invocation error for " + processName + " (" + e.getMessage() + ")?");
            return;
        }

        // Made it all the way through successfully through the maze of exceptions
        sendProcess(newProcess, liveSlaveIds.get(nextToSend));
        nextToSend = (nextToSend + 1) % liveSlaveIds.size();
    }

    /**
     * Serialize newProcess and send it to the next slave machine
     * @param newProcess MigratableProcess
     * @param slaveId int
     */
    private void sendProcess(MigratableProcess newProcess, int slaveId) {
        // Create new ServerMessage indicating for the receiving slave to run a new process
        ServerMessage message = new ServerMessage(ServerMessage.MessageType.RUN, newProcess);
        Socket currentSocket = liveSockets.get(slaveId);
        try {
            ObjectOutputStream sockOut = new ObjectOutputStream(currentSocket.getOutputStream());
            sockOut.writeObject(message);
            sockOut.flush();
            System.out.println("Sent msg " + message + " to slave " + slaveId);
        } catch (IOException e) {
            System.err.println("Error: error serializing msg " + message + "(" + e.getMessage() + ").");
        }
    }

    /**
     * Print the names of all active processes and where they live
     */
    public void listProcesses() {
        System.out.println("MAKE ME PLEASE!!!");
    }

    /**
     * Ping every slave to see which ones are alive still
     */
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
//                System.out.println("Pinged " + sentMessage + " to slave " + i);

                // Wait for response, if valid, continue to next one
                ObjectInputStream sockIn = new ObjectInputStream(s.getInputStream());
                recMessage = (ServerMessage)sockIn.readObject();
                if(recMessage.getType() == ServerMessage.MessageType.PING && recMessage.getPayload().equals(i)) {
//                    System.out.println("Slave " + i + " responded!!");
                    statusOut += " " + i;
                }
            } catch (IOException e) {
                // Slave didn't respond, presumed dead
                System.out.println("Slave " + i + " did not respond in time, presumed lost.");
                deadSlaveIds.add(i);
            } catch (ClassNotFoundException e) {
                System.err.println("Error: Slave received an object that wasn't a ServerMessage (" + e.getMessage() + ")");
            }
        }
        System.out.println(statusOut + " ]");

        // Remove all dead slaves from the lists of the living ones (RIP)
        removeDeadSlaves(deadSlaveIds);
    }

    /**
     * Removes any IDs and Sockets associated with dead slaves
     * @param deadSlaveIds
     * TODO: remove process association too?
     */
    private void removeDeadSlaves(List<Integer> deadSlaveIds) {
        for(Integer i : deadSlaveIds) {
            int idPosition = liveSlaveIds.indexOf(i);
            if(idPosition >= 0) {
                liveSlaveIds.remove(idPosition);
            }
            liveSockets.remove(i);
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
            //TODO: close all slaves attached also
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