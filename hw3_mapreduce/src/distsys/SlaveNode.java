package distsys;

import distsys.kdfs.DataNode;
import distsys.msg.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class SlaveNode extends Thread {

    private ServerSocket slaveServer;
    private DataNode dataNode;
    private boolean running;
    private int slaveNum;

    public SlaveNode(int port) throws IOException {
        String HOSTNAME = InetAddress.getLocalHost().getHostName();
        System.out.println("Starting SlaveNode at " + HOSTNAME + ":" + port);
        slaveServer = new ServerSocket(port);

//        masterHandle = new CommHandler(slaveServer.accept());
        running = true;

        // Ping Master to say hi and give initial BlockMap
        init();
    }


    private void init() throws IOException {
        // Wait for MasterNode to say hi
        CommHandler initHandle = new CommHandler(slaveServer.accept());
        BlockMapMessage initMsg = (BlockMapMessage) initHandle.receiveMessage();


        // Initialize data node
        slaveNum = initMsg.getHostnum();
        dataNode = new DataNode(slaveNum);
        System.out.println("Connected as SlaveNode " + slaveNum);
        initHandle.sendMessage(new BlockMapMessage(slaveNum, dataNode.generateBlockMap()));
    }


    /**
     * Handle an incoming socket connection for SlaveNode
     *
     * @param comm CommHandler that is sending a message to this Slave
     */
    private void handleConnection(CommHandler comm) throws IOException {
        // Receive the message that is being sent
        Message msgIn = comm.receiveMessage();

        // Handle the message received
        switch (msgIn.getType()) {
            case BLOCKMAP:
                // BlockMap requested, send back to MasterNode
                slaveNum = ((BlockMapMessage) msgIn).getHostnum();
                comm.sendMessage(new BlockMapMessage(slaveNum, dataNode.generateBlockMap()));
                break;
            case BLOCK_REQ:
                // A block was requested, read it and send its contents back
                BlockReqMessage blockReq = (BlockReqMessage) msgIn;
                String contents = dataNode.readBlock(blockReq.getBlockId(), blockReq.getOffset());
                comm.sendMessage(new BlockContentMessage(blockReq.getBlockId(), contents));
                break;
            case BLOCK_CONTENT:
                // Someone wants you to write a new block to the file system
                BlockContentMessage blockContent = (BlockContentMessage) msgIn;
                dataNode.writeBlock(blockContent.getBlockID(), blockContent.getBlockContents());
                //TODO: send acknowledgement back?
                break;
            case KILL:
                // Stop running the SlaveNode
                running = false;
                slaveServer.close();
                System.out.println("\nEnding now, byee!");
                break;
            case ACK:
                // Acknowledgement from something
                System.out.println("Someone acknowledged my existence. :3");
                break;
            default:
                System.out.println("SlaveNode: unhandled message type " + msgIn.getType());
                break;
        }

    }


    /**
     * SlaveNode thread loop
     */
    public void run() {
        while (running) {
            try {
                final Socket sock = slaveServer.accept();

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
                System.err.println("Error: oops, an error in the SlaveNode thread! (" + e.getMessage() + ").");
                break;
            }
        }
    }


    /**
     * Main Method
     */
    public static void main(String[] args) throws IOException {
        int PORT = Config.DATA_PORT;
        // Initialize connection with Master
        if (args.length == 1) {
            PORT = Integer.parseInt(args[0]);
        }

        // Run the SlaveNode
        SlaveNode slave = new SlaveNode(PORT);
        slave.start();
    }

}
