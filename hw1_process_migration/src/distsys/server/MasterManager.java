package distsys.server;

import java.io.IOException;
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
        while(listening) {
            try {
                Socket newConnection = sock.accept();
                connections.add(newConnection);
                System.out.println("Added new connection from " + newConnection.getInetAddress().getCanonicalHostName() + "!");
            } catch (IOException e) {
                System.err.println("Error: could not accept connection at local port " + sock.getLocalPort());
            }
        }
    }

    /**
     * Close master socket and slaves attached to it
     */
    public void close() {
        try {
            sock.close();
            //TODO: close all slaves attached also
        } catch(IOException e) {
            System.err.println("Error: problem closing master socket ports.\n" + e.getMessage());
        }
    }

    @Override
    public void run() {
        listen();
        close();
        listening = false;
    }
}
