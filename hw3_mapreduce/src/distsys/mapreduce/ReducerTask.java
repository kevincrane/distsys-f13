package distsys.mapreduce;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/13/13
 */

public class ReducerTask extends Task {
    private int reducerNum;
    private Reducer reducer;
    private List<Integer> mapperJobIds;

    public ReducerTask(Reducer reducer, int jobID, int slaveID, int reducerNum, List<Integer> mapperJobIds) {
        super(jobID, slaveID);
        this.reducer = reducer;
        this.mapperJobIds = mapperJobIds;
        this.reducerNum = reducerNum;

    }

    public Reducer getReducer() {
        return reducer;
    }

    public List<Integer> getDependentMapperJobIds() {
        return mapperJobIds;
    }

    public int getReducerNum() {
        return reducerNum;
    }

    @Override
    public String toString() {
        return super.toString() + "\n MapperJobDependencies: " + mapperJobIds;
    }

}
