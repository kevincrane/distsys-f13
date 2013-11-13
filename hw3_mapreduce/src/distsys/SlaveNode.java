package distsys;

import distsys.kdfs.DataNode;
import distsys.kdfs.DistFile;
import distsys.mapreduce.*;
import distsys.msg.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
        System.out.println("Connected as SlaveNode " + slaveNum);
        initHandle.sendMessage(new BlockMapMessage(slaveNum, dataNode.generateBlockMap()));
    }


    /**
     * Run a ReduceTask on the SlaveNode
     * Has three phases: partition/shuffle, sort, reduce
     *
     * @param reduceTask Which Reduce operation you should run
     *                   TODO: any way to generify Records here, rather than always be String? probably don't care for now
     */
    public List<Record> runReducer(ReducerTask reduceTask) {
        // 1. Partition - Ping each Slave and ask Records that belong to task ID and this partition/slaveNum
        List<Record<String, String>> partitionedRecords = new ArrayList<Record<String, String>>();
        for (String[] slave : Config.SLAVE_NODES) {
            try {
                // Ask for the result partitions from each slave for this Job ID
                CommHandler requestHandle = new CommHandler(slave[0], slave[1]);
                requestHandle.sendMessage(new ResultPartitionMessage(reduceTask.getReducerNum(),
                        reduceTask.getDependentMapperJobIds(), null));
                ResultPartitionMessage partitionMessage = (ResultPartitionMessage) requestHandle.receiveMessage();
                partitionedRecords.addAll(partitionMessage.getPartitionedRecords());
                System.out.println("Now have " + partitionedRecords.size() + " records in reducer partition.");
            } catch (IOException e) {
                // Slave isn't running; Smarter way to just ping slaves that are alive?
            }
        }

        // 2. Sort - Sort partitionedRecords by key and merge like keys together
        List<Record<String, List<String>>> reducerRecords = new ArrayList<Record<String, List<String>>>();
        Collections.sort(partitionedRecords, new Comparator<Record<String, String>>() {
            @Override
            public int compare(Record<String, String> record, Record<String, String> record2) {
                return record.getKey().compareTo(record2.getKey());
            }
        });

        // Merge the sorted records by key
        String currentKey = partitionedRecords.get(0).getKey();
        List<String> currentValues = new ArrayList<String>();
        for (Record<String, String> partitionedRecord : partitionedRecords) {
            if (partitionedRecord.getKey().equals(currentKey)) {
                // Keys match, add value to list
                currentValues.add(partitionedRecord.getValue());
            } else {
                // Moved on to the next key, add previous Records to reducerRecords
                reducerRecords.add(new Record<String, List<String>>(currentKey, currentValues));
                currentKey = partitionedRecord.getKey();
                currentValues = new ArrayList<String>();
            }
        }

        // 3. Reduce - Perform reduce operation on every record (key -> list of all values for that key)
        Reducer reducer = reduceTask.getReducer();
        for (Record<String, List<String>> reducerRecord : reducerRecords) {
            reducer.reduce(reducerRecord.getKey(), reducerRecord.getValue());
        }

        // Done! Return and alert Master that you've finished your job
        return reducer.getReduceOutput();
    }

    /**
     * Read the completed records from the Mapper tasks out of temp files; hash their keys and return them
     * if the hash would assign them to a given reducer number
     *
     * @param reducerNum Which reducer receives these records
     * @param taskIDs    Which task IDs you should look for in local temp storage
     * @return A list of Records that belong to reducerNum
     */
    public List<Record<String, String>> getPartitionedRecords(int reducerNum, List<Integer> taskIDs) {
        // Iterate through each desired task ID, partition if you have it
        List<Record<String, String>> partitionedRecords = new ArrayList<Record<String, String>>();
        for (Integer taskID : taskIDs) {
            // Try to read a mapper result file with task ID; skips if can't open because you don't have it
            try {
                String resultFileName = String.format("%s%03d", Config.MAP_RESULTS, taskID);
                BufferedReader br = new BufferedReader(new FileReader(resultFileName));
                System.out.println("Opened Mapper result file " + resultFileName);
                String recordLine;
                while ((recordLine = br.readLine()) != null) {
                    // Split record line into key and value by tab
                    int tabIndex = recordLine.indexOf('\t');
                    if (tabIndex < 0) continue;

                    // Read key/value, partition by key, store if it fits with right reducer
                    String key = recordLine.substring(0, tabIndex);
                    int partition = Partitioner.getPartition(key, Config.NUM_REDUCERS);
                    if (partition == reducerNum) {
                        String value = recordLine.substring(tabIndex + 1);
                        partitionedRecords.add(new Record<String, String>(key, value));
                        //TODO: make this work with any object? Not possible while reading from text file
                    }
                }
                br.close();
            } catch (IOException ignored) {
            }
        }

        return partitionedRecords;
    }


    //TODO KEVIN: REMOVE

    /**
     * Run a Map or Reduce task and report back to Master after completion
     *
     * @param taskMsg The MapReduce task to run
     */
    private void runMapReduceTask(TaskMessage taskMsg) {
        if (taskMsg.getTask() instanceof MapperTask) {
            //TODO: handle mapper from somewhere; make this into a new method
            System.out.println("Slave is handling Map task.");
        } else if (taskMsg.getTask() instanceof ReducerTask) {
            // Handle Reduce task
            List<Record> reducerResults = runReducer((ReducerTask) taskMsg.getTask());
            // TODO Send response back to Master, store in KDFS
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
                //TODO: send acknowledgement back?
                break;
            case TASK:
                // Mapper or Reducer Task coming in to be run on this slave
                Task task = ((TaskMessage) msgIn).getTask();
                if (task instanceof MapperTask) {
                    MapperTask mapperTask = (MapperTask) task;
                    // set the dataNode of the DistFile to the current slave's DataNode
                    mapperTask.setDataNode(dataNode);
                    new MapTaskProcessor(mapperTask, comm);
                } else if (task instanceof  ReducerTask) {
                    // TODO PERFORM REDUCE
                } else {
                    System.out.println("Received unknown task, ignoring.");
                }
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
                DistFile file = new DistFile("alice.txt", 636, 16384);
                file.setDataNode(dataNode);
//                file.seek(0);
                System.out.println();
                Record<Integer, String> record = file.nextRecord();
                do {
                    System.out.println(record.getKey() + "=" + record.getValue());
                    record = file.nextRecord();
                } while (record != null);

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
                            System.err.println("Error: Did not handle request from incoming msg properly (" +
                                    e.getMessage() + ").");
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

        // Run the SlaveNode
        SlaveNode slave = new SlaveNode(PORT);
        slave.start();
    }

}
