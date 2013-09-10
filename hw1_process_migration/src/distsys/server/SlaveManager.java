package distsys.server;

import distsys.process.MigratableProcess;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static distsys.server.ServerMessage.MessageType.*;

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
        sockOut = new ObjectOutputStream(sock.getOutputStream());
        processList = new ArrayList<MigratableProcess>();
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
            switch(newMessage.getType()) {
                case RUN:
                    receiveProcess((MigratableProcess)newMessage.getPayload());
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
     * @param sockIn
     */
    private void receiveProcess(MigratableProcess newProcess) {
        processList.add(newProcess);
        Thread processThread = new Thread(newProcess);
        processThread.start();

        //TODO:
//            ADD USER INPUT TO SELECT OPTIONS ON ProcessManager
//            REMOVE TEST PROCESS (ProcessManager), PLUS SLEEPING SECTION (MasterManager)
//            MAKE ps FEATURE


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
