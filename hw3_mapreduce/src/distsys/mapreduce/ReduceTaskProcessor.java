package distsys.mapreduce;

import distsys.Config;
import distsys.msg.CommHandler;
import distsys.msg.ResultPartitionMessage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 * Time: 4:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class ReduceTaskProcessor extends TaskProcessor {
    private ReducerTask task;

    public ReduceTaskProcessor(ReducerTask reducerTask, CommHandler commHandler) {
        super(reducerTask, commHandler);
        this.task = reducerTask;
    }

    public void run() {
        // TODO: KEVIN REDUCING HERE

        List<Record> reducerResults = runReducer(task);
        // TODO Send response back to Master, store in KDFS
        // TODO PERFORM REDUCE
        // TODO Don't forget to also send a TASKUPDATE message with the results to Master so that Co-ordinator will remove the REDUCER
        // TODO task from the queue
        // NOTE: KEVIN - TASKUPDATE Message also takes in a payload to which the results of the last reduce can be attached to
        // send to master for processing.
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
}
