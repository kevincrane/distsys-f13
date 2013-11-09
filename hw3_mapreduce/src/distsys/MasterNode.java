package distsys;

import distsys.kdfs.NameNode;
import distsys.msg.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class MasterNode extends Thread {

    // List of all actively connected Slave nodes' CommHandlers
    private ServerSocket masterServer;
    private boolean running;

    // Namenode of KDFS
    private NameNode namenode;

    public MasterNode() throws IOException {
        masterServer = new ServerSocket(Config.DATA_PORT);
        namenode = new NameNode();

        running = true;
    }


    int i = 1;
    /**
     * Handle an incoming socket connection for MasterNode
     * @param comm    CommHandler that is sending a message to the Master
     */
    private void handleConnection(CommHandler comm) throws IOException {
        // Receive the message that is being sent
        Message msgIn = comm.receiveMessage();

        // Handle the message received
        switch (msgIn.getType()) {
            case BLOCKMAP:
                namenode.updateBlockMap(((BlockMapMessage) msgIn).getHostnum(), ((BlockMapMessage) msgIn).getBlocks());
                comm.sendMessage(new AckMessage());

                //TODO remove
                CommHandler tempHandle = new CommHandler(Config.SLAVE_NODES[0][0], Config.SLAVE_NODES[0][1]);
                System.out.println("Sending request to " + tempHandle.getHostname() + ":" + tempHandle.getPort());
                tempHandle.sendMessage(new BlockContentMessage(12+i, "HIHIHI I'M THE BEST!!"));

//                System.out.print("REQUESTED " + i + ": ");
//                tempHandle.sendMessage(new BlockReqMessage(i));
//                BlockContentMessage contents = (BlockContentMessage) tempHandle.receiveMessage();
//                System.out.println(contents.getBlockContents());
//                i++;
                break;
            case BLOCK_ADDR:
                //TODO
                System.err.println("Handling request " + msgIn.getType());
                namenode.retrieveBlockAddress(comm, ((BlockAddrMessage) msgIn).getBlockId());
                break;
            default:
                System.out.println("MasterNode: unhandled message type " + msgIn.getType());
                break;
        }

    }


    /** MasterNode thread loop */
    public void run() {
        while(running) {
            try {
                final Socket sock = masterServer.accept();
                System.err.println("Received connection from port " + sock.getPort());
//                handleConnection(new CommHandler(sock));

                // Handle this request in a new thread
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            handleConnection(new CommHandler(sock));
                        } catch (IOException e) {
                            System.err.println("Error: Did not handle request from incoming msg properly (" + e.getMessage() + ").");
                        }
                    }
                }).start();
            } catch (IOException e) {
                System.err.println("Error: oops, an error in the MasterNode thread! (" + e.getMessage() + ").");
            }
        }
    }

}
