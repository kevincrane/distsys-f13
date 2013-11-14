package distsys;

import distsys.kdfs.DataNode;
import distsys.mapreduce.*;
import distsys.msg.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
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
        System.out.println("Connected as SlaveNode " + slaveNum + ".\n");
        initHandle.sendMessage(new BlockMapMessage(slaveNum, dataNode.generateBlockMap()));
    }

    /**
     * Run a Map or Reduce task and report back to Master after completion
     *
     * @param taskMsg The MapReduce task to run
     */
    private void runMapReduceTask(TaskMessage taskMsg, CommHandler comm) {
        Task task = taskMsg.getTask();
        if (task instanceof MapperTask) {
            MapperTask mapperTask = (MapperTask) task;
            // set the dataNode of the DistFile to the current slave's DataNode
            mapperTask.setDataNode(dataNode);
            new MapTaskProcessor(mapperTask, comm).start();
        } else if (task instanceof ReducerTask) {
            ReducerTask reducerTask = (ReducerTask) task;
            new ReduceTaskProcessor(reducerTask, comm).start();
        } else {
            String clazz = "NULL";
            if (task != null) {
                clazz = task.getClass().toString();
            }
            System.out.println("Received unknown task of type: " + clazz + ", ignoring.");
        }
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
                //TODO don't generate BlockMap for every ping, wastes resources every 5 seconds,
                // updated automatically in master during puts, don't need it to update constantly
                comm.sendMessage(new BlockMapMessage(slaveNum, dataNode.generateBlockMap()));
                break;
            case BLOCK_REQ:
                // A block was requested, read it and send its contents back
                // we set global fetching to false since in this case we are only checking locally
                BlockReqMessage blockReq = (BlockReqMessage) msgIn;
                String contents = dataNode.readBlock(blockReq.getBlockId(), blockReq.getOffset(), false);
                comm.sendMessage(new BlockContentMessage(blockReq.getBlockId(), contents));
                break;
            case BLOCK_CONTENT:
                // Someone wants you to write a new block to the file system
                BlockContentMessage blockContent = (BlockContentMessage) msgIn;
                dataNode.writeBlock(blockContent.getBlockID(), blockContent.getBlockContents());
                break;
            case BLOCK_ADDR:
                // Master node is requesting this slave to copy over a blockId if it can
                BlockAddrMessage replicateMessage = (BlockAddrMessage) msgIn;
                // Get blockContents from nodes provided by master in the message
                String blockContents = dataNode.readBlock(replicateMessage.getBlockId(), 0, true, replicateMessage);
                // write the block to the slave
                dataNode.writeBlock(replicateMessage.getBlockId(), blockContents);
                break;
            case TASK:
                // Mapper or Reducer Task coming in to be run on this slave
                runMapReduceTask((TaskMessage) msgIn, comm);
                break;

            case PARTITION:
                // Read and partition all records from a completed Mapper task
                ResultPartitionMessage partMsg = (ResultPartitionMessage) msgIn;
                List<Record<String, String>> partRecords = getPartitionedRecords(partMsg.getReducerNum(), partMsg.getTaskIDs());
                comm.sendMessage(new ResultPartitionMessage(partMsg.getReducerNum(), null, partRecords));
                break;

            case KILL:
                // Stop running the SlaveNode
                running = false;
                slaveServer.close();
                System.out.println("\nEnding now, byee! <3");
                break;
            case ACK:
                // Acknowledgement from something

                //TODO remove this
//                DistFile file = new DistFile("alice.txt", 636, 16384);
//                file.setDataNode(dataNode);
////                file.seek(0);
//                System.out.println();
//                Record<Integer, String> record = file.nextRecord();
//                do {
//                    System.out.println(record.getKey() + "=" + record.getValue());
//                    record = file.nextRecord();
//                } while (record != null);
                break;
            default:
                System.out.println("SlaveNode: unhandled message type " + msgIn.getType());
                break;
        }

    }


    /**
     * Read the completed records from the Mapper tasks out of temp files; hash their keys and return them
     * if the hash would assign them to a given reducer number
     *
     * @param partitionNum Which reducer receives these records
     * @param taskIDs      Which task IDs you should look for in local temp storage
     * @return A list of Records that belong to reducerNum
     */
    public List<Record<String, String>> getPartitionedRecords(int partitionNum, Set<Integer> taskIDs) {
        // Iterate through each desired task ID, partition if you have it
        List<Record<String, String>> partitionedRecords = new ArrayList<Record<String, String>>();
        for (Integer taskID : taskIDs) {
            // Try to read a mapper result file with task ID; skips if can't open because you don't have it
            try {
                String resultFileName = String.format("%s%d%03d", Config.MAP_RESULTS, slaveNum, taskID);    //TODO here
                BufferedReader br = new BufferedReader(new FileReader(resultFileName));
//                System.out.println("Opened Mapper result file " + resultFileName);

                String recordLine;
                while ((recordLine = br.readLine()) != null) {
                    // Split record line into key and value by tab
                    int tabIndex = recordLine.indexOf('\t');
                    if (tabIndex < 0) continue;

                    // Read key/value, partition by key, store if it fits with right reducer
                    String key = recordLine.substring(0, tabIndex);
                    int partition = Partitioner.getPartition(key, Config.NUM_REDUCERS);
                    if (partition == partitionNum) {
                        String value = recordLine.substring(tabIndex + 1);
                        partitionedRecords.add(new Record<String, String>(key, value));
                        //TODO: make this work with any object? Not possible while reading from text file probably
                    }
                }
                br.close();
            } catch (IOException ignored) {
            }
        }

        return partitionedRecords;
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
                            System.err.println("Error: Did not handle request from incoming msg properly (" +
                                    e.getMessage() + ").");
                            e.printStackTrace();
                        }
                    }
                }).start();
            } catch (IOException e) {
                if (running) {
                    System.err.println("Error: oops, an error in the SlaveNode thread! (" + e.getMessage() + ").");
                }
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

        //create data and tmp directories if they don't exist
        new File(Config.BLOCK_FOLDER).mkdirs();
        new File(Config.MAP_RESULTS_FOLDER).mkdirs();

        // Run the SlaveNode
        SlaveNode slave = new SlaveNode(PORT);
        slave.start();
    }

}
