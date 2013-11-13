package distsys.msg;

import distsys.mapreduce.Record;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/6/13
 */
public class ResultPartitionMessage extends Message {

    private int reducerNum;
    private List<Integer> taskIDs;
    private List<Record<String, String>> partitionedRecords;

    // Ask for and return the partitioned results from a previously run map job
    public ResultPartitionMessage(int reducerNum, List<Integer> taskIDs, List<Record<String, String>> records) {
        super(MessageType.PARTITION, reducerNum);
        this.reducerNum = reducerNum;
        this.taskIDs = taskIDs;
        this.partitionedRecords = records;
    }

    public int getReducerNum() {
        return reducerNum;
    }

    public List<Integer> getTaskIDs() {
        return taskIDs;
    }

    public List<Record<String, String>> getPartitionedRecords() {
        return partitionedRecords;
    }
}
