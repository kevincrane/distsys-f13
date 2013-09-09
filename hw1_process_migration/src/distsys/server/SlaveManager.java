package distsys.server;

import distsys.process.MigratableProcess;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/9/13
 */
public class SlaveManager {
    private List<MigratableProcess> processList;
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
        sockIn = new ObjectInputStream(sock.getInputStream());
        sockOut = new ObjectOutputStream(sock.getOutputStream());
        System.out.println("Connected to socket at " + hostname + ":" + port + "!");

        // TODO: Send message to Master to introduce yourself
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
}
