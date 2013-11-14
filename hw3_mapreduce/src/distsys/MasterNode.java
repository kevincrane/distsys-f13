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
    private int currentJobId;
    // Keeps track of what reducers are related so that in we can tell the user when their task is finished
    private List<List<Integer>> relatedReducers;

    // Namenode of KDFS
    private NameNode namenode;

    public MasterNode() throws IOException {
        int currentJobId = 0;
        relatedReducers = new ArrayList<List<Integer>>();
        masterServer = new ServerSocket(Config.DATA_PORT);
        namenode = new NameNode();
        coordinator = new Coordinator(namenode, this);

        running = true;

        pingTimer = new Timer();
        pingTimer.schedule(new TimerTask() {
            public void run() {
                Map<Integer, Set<Integer>> oldBlockMap = namenode.blockMap;
                // Ping slaves if any of them are dead, notify Co-Ordinator
                List<Integer> deadSlaveIds = namenode.pingSlaves();
                coordinator.processDeadSlaveEvent(deadSlaveIds);
                // Ensure Replication Factor best effort by replicating blocks that have dissappeared
                List<Integer> blockIds = new ArrayList<Integer>();
                for (int deadSlaveId: deadSlaveIds) {
                    Set<Integer> slaveBlockIds = oldBlockMap.get(deadSlaveId);
                    if (slaveBlockIds != null)
                        blockIds.addAll(slaveBlockIds);
                }
                replicateBlocksOnce(blockIds);
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
     * Get running mapreduce jobs within all slaves
     * @return Map from slave id to a list of tasks that it is currently running
     */
    public HashMap<Integer, List<Task>> getRunningMapReduceJobs() {
        HashMap<Integer, List<Task>> runningJobMap = new HashMap<Integer, List<Task>>();
        for (int slaveId: namenode.getSlaveIds()) {
            runningJobMap.put(slaveId, coordinator.getRunningTasks(slaveId));
        }
        return runningJobMap;
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
     * Best effort attempt to replicate given block ids one more time
     * @param blockIds blockIds that need to be duplicated once in another slave
     */
    private void replicateBlocksOnce(List<Integer> blockIds) {
        if (blockIds.size() > 0)
            System.out.println("Detected slave(s) down, if necessary, best effort replicating blockIds: " + blockIds);
        for (int blockId: blockIds) {
            for (int slaveWithoutBlock: namenode.getSlavesWithoutBlock(blockId)) {
                System.out.println("Sending Replicate message to slave " + slaveWithoutBlock + " for blockId " + blockId);
                // For one random slave without that block, ask him to replicate it
                try {
                    CommHandler comm = new CommHandler(Config.SLAVE_NODES[slaveWithoutBlock][0], Config.SLAVE_NODES[slaveWithoutBlock][1]);
                    comm.sendMessage(new BlockAddrMessage(new ArrayList<Integer>(namenode.getSlavesWithBlock(blockId)), blockId));
                } catch (IOException ignored) {
                    //we try every slave that doesn't have the block until one of them gets the message
                    //if all slaves without the block are down, there is no point so we ignored the exception
                }
                break;
            }
        }
    }

    /**
     * Notify master that a reducer task is done so that we can tell the user when all reducers of a related task
     * are completed and hence the user can check the output file
     * @param task ReducerTask that has been completed according to the CoOrdinator
     */
    public void notifyReducerTaskDone(ReducerTask task) {
        for (int i=0; i<relatedReducers.size(); i++) {
            List<Integer> relatedReducerList = relatedReducers.get(i);
            for (int j=0; j<relatedReducerList.size(); j++) {
                int reducerJobId = relatedReducerList.get(j);
                if (reducerJobId == task.getJobID()) {
                    relatedReducerList.remove(j);
                }
            }
            if (relatedReducerList.size() == 0) {
                relatedReducers.remove(i);
                System.out.println("\n Congratulations! Your MapReduce task has been completed. Please check the results in the output file: " + task.getOutputFile());
            }
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

        List<Integer> jobReducers = new ArrayList<Integer>();
        // Add reducers to scheduled tasks
        for (int i = 0; i < Config.NUM_REDUCERS; i++) {
            ReducerTask reducerTask = new ReducerTask(newJob.getReducer(), newJob.getOutputFile(), currentJobId++, -1, i, mapperJobIds);
            tasks.add(reducerTask);
            jobReducers.add(reducerTask.getJobID());
        }
        relatedReducers.add(jobReducers);

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
