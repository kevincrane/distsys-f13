package distsys.mapreduce;

import distsys.Config;
import distsys.msg.CommHandler;
import distsys.msg.ResultPartitionMessage;
import distsys.msg.TaskUpdateMessage;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/13/13
 */
public class ReduceTaskProcessor extends TaskProcessor {
    private ReducerTask task;

    public ReduceTaskProcessor(ReducerTask reducerTask, CommHandler commHandler) {
        super(reducerTask, commHandler);
        this.task = reducerTask;
    }

    public void run() {
        List<Record> reducerResults = runReducer(task);

        // We are DONE, TELL MASTER we are done, we retry in case of failure for MAX_SOCKET_TRIES times
        int triesLeft = Config.MAX_SOCKET_TRIES;
        while (triesLeft > 0) {
            try {
                CommHandler masterComm = new CommHandler(Config.MASTER_NODE, Config.DATA_PORT);
                masterComm.sendMessage(new TaskUpdateMessage(task.getJobID(), false, true, reducerResults));
                System.out.println("Sent message to master, Reduce job with id " + task.getJobID() + " is done.");
                break;
            } catch (IOException e) {
                triesLeft--;
                e.printStackTrace();
                if (triesLeft == 0) {
                    System.err.println("ERROR: Could not send any messages to master, master is down.");
                }
            }
        }
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
                requestHandle.sendMessage(new ResultPartitionMessage(reduceTask.getPartitionNum(),
                        new HashSet<Integer>(reduceTask.getDependentMapperJobIds()), null));
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

        if (partitionedRecords.size() == 0) {
            System.err.println("Error: couldn't find any records for partition " + reduceTask.getPartitionNum() + " to reduce.");
            return null;
        }

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
}
