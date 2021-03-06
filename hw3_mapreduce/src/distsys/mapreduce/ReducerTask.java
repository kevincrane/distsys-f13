package distsys.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/13/13
 */

public class ReducerTask extends Task {
    private int partitionNum;
    private Reducer reducer;
    private String outputFile;
    // Keeps track of whether job is running or not
    private HashMap<Integer, Boolean> mapperJobsStatus;

    public ReducerTask(Reducer reducer, String output, int jobID, int slaveID, int partitionNum, List<Integer> mapperJobIds) {
        super(jobID, slaveID);
        this.reducer = reducer;
        this.partitionNum = partitionNum;
        this.outputFile = output;

        mapperJobsStatus = new HashMap<Integer, Boolean>();
        for (int mapperJobId : mapperJobIds) {
            mapperJobsStatus.put(mapperJobId, false);
        }
    }

    public Reducer getReducer() {
        return reducer;
    }

    /**
     * @return JobIds of mappers whom this reducer is depenedent on
     */
    public Set<Integer> getDependentMapperJobIds() {
        return mapperJobsStatus.keySet();
    }

    /**
     * Method ensures new dependent mappers are not added to Reducer, only changes status of existing mapper jobs
     *
     * @param jobId  Id of the mapper job whose status you wish to change
     * @param isDone current running status of that mapper, true: that mapper done, false: mapper is still running
     */
    public void setMapperJobStatus(int jobId, boolean isDone) {
        if (mapperJobsStatus.containsKey(jobId)) {
            mapperJobsStatus.put(jobId, isDone);
        }
    }

    /**
     * Is the reducer is ready to be executed? Which means every mapper that it is dependent on should have completed
     *
     * @return boolean indicating whether all the mappers that the reducer depends on are completed
     */
    public boolean allMappersAreReady() {
        boolean mappersReady = true;
        for (int jobID : getDependentMapperJobIds()) {
            mappersReady &= mapperJobsStatus.get(jobID);
        }
        return mappersReady;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public String getOutputFile() {
        return outputFile;
    }

    @Override
    public String toString() {
        return "Reducer" + super.toString() + "\n MapperJobDependencies: " + new ArrayList<Integer>(getDependentMapperJobIds());
    }

}
