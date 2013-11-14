package distsys;

import distsys.kdfs.BlockInfo;
import distsys.kdfs.DistFile;
import distsys.kdfs.NameNode;
import distsys.mapreduce.*;
import distsys.msg.*;

import java.io.File;
import java.io.FileWriter;
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
    private Coordinator coordinator;
    private int currentJobId = 0;

    // Namenode of KDFS
    private NameNode namenode;

    public MasterNode() throws IOException {
        masterServer = new ServerSocket(Config.DATA_PORT);
        namenode = new NameNode();
        coordinator = new Coordinator(namenode);

        running = true;

        pingTimer = new Timer();
        pingTimer.schedule(new TimerTask() {
            public void run() {
                // Ping slaves if any of them are dead, notify Co-Ordinator
                List<Integer> deadSlaveIds = namenode.pingSlaves();
                coordinator.processDeadSlaveEvent(deadSlaveIds);
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
                break;
            case BLOCK_ADDR:
                // Retrieve the DataNode address of a Block ID and send it back
                namenode.retrieveBlockAddress(comm, ((BlockAddrMessage) msgIn).getBlockId());
                break;
            case BLOCK_POS:
                // Determine which block contains a given position in a file and return the remainder of that block
                BlockPosMessage blockPosMessage = (BlockPosMessage) msgIn;
                namenode.returnBlockIdByPosition(comm, blockPosMessage.getFileName(), blockPosMessage.getBlockStart());
                break;
            case TASK_UPDATE:
                // send update on mapreduce task status to the CoOrdinator
                coordinator.processTaskUpdateMessage((TaskUpdateMessage) msgIn);
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
        // Input File has to be specified
        if (newJob.getInputFile() == null) {
            System.out.println("Error: no input file specified for mapreduce job");
            return;
        }

        // Load input file into KDFS if it doesn't already exist
        if (!namenode.listFiles().contains(newJob.getInputFile())) {
            int blocksWritten = namenode.putFile(newJob.getInputFile());
            if (blocksWritten == 0) {
                System.out.println("Error: could not write file " + newJob.getInputFile() + " to file system.");
                return;
            }
        }

        // Prepare output file (open and empty)
        try {
            File outputFile = new File(newJob.getOutputFile());
            FileWriter fw = new FileWriter(outputFile);
            fw.write("");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Integer> blockIds = namenode.getFileBlockIds(newJob.getInputFile());
        List<Task> tasks = new ArrayList<Task>();
        List<Integer> mapperJobIds = new ArrayList<Integer>();

        // Add mappers to scheduled tasks
        for (int blockId : blockIds) {
            BlockInfo blockInfo = namenode.getBlockInfo(blockId);
            int startPosition = blockInfo.getOffset();
            // all tasks are initially set as slaveId -1 and then scheduled to the right slave by the Co-ordinator
            tasks.add(new MapperTask(
                    newJob.getMapper(),
                    new DistFile(newJob.getInputFile(), startPosition, startPosition + blockInfo.getFileLen()),
                    currentJobId,
                    -1
            ));
            mapperJobIds.add(currentJobId++);
        }

        // Add reducers to scheduled tasks
        for (int i = 0; i < Config.NUM_REDUCERS; i++) {
            tasks.add(new ReducerTask(newJob.getReducer(), newJob.getOutputFile(), currentJobId++, -1, i, mapperJobIds));
        }

        coordinator.scheduleTasks(tasks);
    }


    /**
     * MasterNode thread loop
     */
    public void run() {
        //TODO remove
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//        try {
//            CommHandler tempHandle = new CommHandler(Config.SLAVE_NODES[0][0], Config.SLAVE_NODES[0][1]);
//            tempHandle.sendMessage(new AckMessage());
//        } catch (IOException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }

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
                            System.err.println("Error: Did not handle request from incoming msg properly (" +
                                    e.getMessage() + ").");
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
