package distsys;

import distsys.kdfs.NameNode;
import distsys.mapreduce.MapReduceJob;
import distsys.msg.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/6/13
 */
public class MasterNode extends Thread {

    // List of all actively connected Slave nodes' CommHandlers
    private ServerSocket masterServer;
    private boolean running;
    private Timer pingTimer;

    // Namenode of KDFS
    private NameNode namenode;

    public MasterNode() throws IOException {
        masterServer = new ServerSocket(Config.DATA_PORT);
        namenode = new NameNode();

        running = true;

        pingTimer = new Timer();
        pingTimer.schedule(new TimerTask() {
            public void run() {
                namenode.pingSlaves();
            }
        }, 5000, 5000);
    }


    /**
     * Handle an incoming socket connection for MasterNode
     *
     * @param comm CommHandler that is sending a message to the Master
     */
    private void handleConnection(CommHandler comm) throws IOException {
        // Receive the message that is being sent
        Message msgIn = comm.receiveMessage();

        // Handle the message received
        switch (msgIn.getType()) {
            case BLOCKMAP:
                // SlaveNode sent a BlockMap to update
                namenode.updateBlockMap(((BlockMapMessage) msgIn).getHostnum(), ((BlockMapMessage) msgIn).getBlocks());
                comm.sendMessage(new AckMessage());

                //TODO remove
//                CommHandler tempHandle = new CommHandler(Config.SLAVE_NODES[0][0], Config.SLAVE_NODES[0][1]);
//                System.out.println("Sending request to " + tempHandle.getHostname() + ":" + tempHandle.getPort());
//                tempHandle.sendMessage(new BlockContentMessage(12 + i, "HIHIHI I'M THE BEST!!"));

//                System.out.print("REQUESTED " + i + ": ");
//                tempHandle.sendMessage(new BlockReqMessage(i));
//                BlockContentMessage contents = (BlockContentMessage) tempHandle.receiveMessage();
//                System.out.println(contents.getBlockContents());
//                i++;
                break;
            case BLOCK_ADDR:
                // Retrieve the DataNode address of a Block ID and send it back
                namenode.retrieveBlockAddress(comm, ((BlockAddrMessage) msgIn).getBlockId());
                break;
            case BLOCK_POS:
                // Determine which block contains a given position in a file and return the remainder of that block
                BlockPosMessage blockPosMessage = (BlockPosMessage) msgIn;
                namenode.getBlockWithPosition(comm, blockPosMessage.getFileName(), blockPosMessage.getBlockStart());
                break;
            default:
                System.out.println("MasterNode: unhandled message type " + msgIn.getType());
                break;
        }
    }

    /**
     * Read file from KDFS
     *
     * @param fileName Name of file to read from KDFS
     * @return File contents
     */
    public String readFile(String fileName) {
        String fileContents = namenode.readFile(fileName);
        if (fileContents == null) {
            System.err.println("NameNode could not read file " + fileName + " from KDFS. :(");
        }
        return fileContents;
    }

    /**
     * Store a new file in KDFS
     *
     * @param fileName Name of file to store
     */
    public void putFile(String fileName) {
        int blocksWritten = namenode.putFile(fileName);
        if (blocksWritten == 0) {
            System.out.println("Error: no blocks written to KDFS file system. :(");
        } else {
            System.out.println("Successfully wrote " + blocksWritten + " to KDFS file system!");
        }
    }

    /**
     * User method: print a list of the files in namespace
     */
    public void listFiles() {
        Set<String> fileNames = namenode.listFiles();
        List<String> sortedFileNames = new ArrayList<String>(fileNames);
        Collections.sort(sortedFileNames);

        System.out.println("KDFS namespace contains:");
        for (String filename : sortedFileNames) {
            System.out.println(filename + " : " + namenode.getFileBlockIds(filename).size() + " blocks");
        }
    }

    /**
     * Start a new Map Reduce job. Store input file to KDFS if it isn't already there, split the job into tasks, and
     * add them to a queue for the Coordinator. Coordinator will eventually send the tasks to DataNodes for processing.
     *
     * @param newJob MapReduce job to be run
     */
    public void newMapReduceJob(MapReduceJob newJob) {
        // Load input file into KDFS if it doesn't already exist
        if (!namenode.listFiles().contains(newJob.getInputFile())) {
            int blocksWritten = namenode.putFile(newJob.getInputFile());
            if (blocksWritten == 0) {
                System.out.println("Error: could not write file " + newJob.getInputFile() + " to file system.");
                return;
            }
        }

        //TODO BALA, start here; split Job into Tasks by how many blocks the file has (namespace.get(newJob.getInputFile()).size())
        //  e.g. 9 blocks -> 9 tasks (Task - Mapper, filename, start index, end index)
        //  distribute the tasks (find the next slave, send a MapperMessage to slave)
        //      Which slaves have which blocks, which slaves are least full, queue of tasks that can't be scheduled yet
        //  On the Slave node
        //      when it receives MapperMessage, execute map, write the output to a file (key \t value\n)
        //      Somehow, send output file to partitioner, send Master a message saying you're done
    }


    /**
     * MasterNode thread loop
     */
    public void run() {
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//        namenode.putFile("testFile1");
//
        //TODO remove
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        try {
            CommHandler tempHandle = new CommHandler(Config.SLAVE_NODES[0][0], Config.SLAVE_NODES[0][1]);
            tempHandle.sendMessage(new AckMessage());
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        while (running) {
            try {
                final Socket sock = masterServer.accept();
//                System.err.println("Received connection from port " + sock.getPort());

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
                if (running) {
                    System.err.println("Error: oops, an error in the MasterNode thread! (" + e.getMessage() + ").");
                }
            }
        }
    }


    /**
     * Close master socket and slaves attached to it
     */
    public void close() {
        running = false;
        try {
            pingTimer.cancel();
            masterServer.close();
            for (int i : namenode.blockMap.keySet()) {
                CommHandler killHandle = new CommHandler(Config.SLAVE_NODES[i][0], Config.SLAVE_NODES[i][1]);
                killHandle.sendMessage(new KillMessage());
            }
        } catch (IOException e) {
            System.err.println("Error: problem closing master socket ports.\n" + e.getMessage());
        }
    }

}
