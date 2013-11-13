package distsys.mapreduce;

import distsys.kdfs.DistFile;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 * Time: 1:32 AM
 * To change this template use File | Settings | File Templates.
 */

public class ReducerTask extends Task {
    private Reducer reducer;
    private List<Integer> mapperJobIds;

    public ReducerTask(Reducer reducer, int jobID, int slaveID, List<Integer> mapperJobIds) {
        super(jobID, slaveID);
        this.reducer = reducer;
        this.mapperJobIds = mapperJobIds;
    }

    public Reducer getReducer() {
        return reducer;
    }

    public List<Integer> getDependentMapperJobIds() {
        return mapperJobIds;
    }

    @Override
    public String toString() {
        return super.toString() + "\n MapperJobDependencies: " + mapperJobIds;
    }

}
