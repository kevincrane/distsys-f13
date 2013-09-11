package distsys.server;

import distsys.process.MigratableProcess;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/9/13
 */
public class MasterManager extends Thread {
    private ServerSocket sock;
    private List<Socket> connections;
    private int nextSlave = 0;
    private boolean listening;


    /**
     * Initialize new Socket connection with Master
     * @param port
     * @throws java.io.IOException
     */
    public MasterManager(int port) throws IOException {
        // Set up socket and port readers to master node
        sock = new ServerSocket(port);
        connections = new ArrayList<Socket>();
        System.out.println("Master is listening for connections at port " + port + "!");
        listening = true;
    }

    /**
     * Listen for new slave nodes to contact master
     */
    public void listen() {
        try {
            Socket newConnection = sock.accept();
            connections.add(newConnection);
            System.out.println("Added new connection from " + newConnection.getInetAddress().getCanonicalHostName() + "!");
        } catch (IOException e) {
            if(listening) {
                System.err.println("Error: could not accept connection at local port " + sock.getLocalPort());
            }
        }
    }

    /**
     * Add a new process to be run by one of the available slaves
     * @param processName
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
        sendProcess(newProcess, nextSlave);
        nextSlave = (nextSlave + 1) % connections.size();   //TODO: improve nextSlave to account for dead slaves (morbid)
    }

    /**
     * Serialize newProcess and send it to the next slave machine
     * @param newProcess
     * @param nextSlave
     */
    private void sendProcess(MigratableProcess newProcess, int nextSlave) {
        // Create new ServerMessage indicating for the receiving slave to run a new process
        ServerMessage message = new ServerMessage(ServerMessage.MessageType.RUN, newProcess);
        Socket currentSocket = connections.get(nextSlave);
        try {
            ObjectOutputStream sockOut = new ObjectOutputStream(currentSocket.getOutputStream());
            sockOut.writeObject(message);
            sockOut.flush();
            System.out.println("Sent msg " + message + " to slave " + nextSlave);
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
     * Close master socket and slaves attached to it
     */
    public void close() {
        listening = false;
        try {
            sock.close();
            for(Socket s : connections) {
                ServerMessage message = new ServerMessage(ServerMessage.MessageType.QUIT, null);
                try {
                    ObjectOutputStream sockOut = new ObjectOutputStream(s.getOutputStream());
                    sockOut.writeObject(message);
                    sockOut.flush();
                    System.out.println("Sent msg " + message + " to slave " + nextSlave);
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