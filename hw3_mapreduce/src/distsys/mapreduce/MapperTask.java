package distsys.mapreduce;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/11/13
 */
public class MapperTask extends Task {

    private MapReduceJob.Mapper mapper;
    private String fileName;
    private int startPos;
    private int endPos;

    protected MapperTask(MapReduceJob.Mapper mapper, String fileName, int startPos, int endPos, int jobID, int slaveID) {
        super(jobID, slaveID);
        this.mapper = mapper;
        this.fileName = fileName;
        this.startPos = startPos;
        this.endPos = endPos;
    }

    //TODO get ahold of a DataNode for DistFile somehow

    public MapReduceJob.Mapper getMapper() {
        return mapper;
    }
}