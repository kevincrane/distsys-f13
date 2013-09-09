package distsys.server;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/9/13
 */
public class MasterManager {
    private ServerSocket sock;


    /**
     * Initialize new Socket connection with Master
     * @param port
     * @throws java.io.IOException
     */
    public MasterManager(int port) throws IOException {
        // Set up socket and port readers to master node
        sock = new ServerSocket(port);
        System.out.println("Master is listening for connections at port " + port + "!");

        // TODO: Send message to Master to introduce yourself
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
}
