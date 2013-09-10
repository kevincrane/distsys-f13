package distsys.server;

import distsys.process.MigratableProcess;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
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
        sockOut = new ObjectOutputStream(sock.getOutputStream());
        processList = new ArrayList<MigratableProcess>();
        System.out.println("Connected to socket at " + hostname + ":" + port + "!");
    }

    /**
     * Listen for messages from Master and act on them
     */
    public void listen() throws IOException {
        sockIn = new ObjectInputStream(sock.getInputStream());

        //TODO: only if message received is to add a new process
        receiveProcess(sockIn);
    }

    /**
     * Receive a serialized process from the socket;
     * Deserialize and run it
     * @param sockIn
     */
    private void receiveProcess(ObjectInputStream sockIn) {
        try {
            MigratableProcess newProcess = (MigratableProcess)sockIn.readObject();;
            processList.add(newProcess);
            Thread processThread = new Thread(newProcess);
            processThread.start();


            //TODO:
//            ADD USER INPUT TO SELECT OPTIONS ON ProcessManager
//            REMOVE TEST PROCESS (ProcessManager), PLUS SLEEPING SECTION (MasterManager)
//            MAKE ps FEATURE


        } catch(IOException e) {
            System.err.println("Error: problem reading from socket ObjectInput stream (" + e.getMessage() + ").");
        } catch(ClassNotFoundException e) {
            System.err.println("Error: received the wrong object type from input stream (" + e.getMessage() + ").");
            e.printStackTrace();
        }
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
