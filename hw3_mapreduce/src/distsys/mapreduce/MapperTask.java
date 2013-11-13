package distsys.mapreduce;

import distsys.kdfs.DistFile;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/11/13
 */

public class MapperTask extends FileTask {

    private Mapper mapper;

    public MapperTask(Mapper mapper, DistFile distFile, int jobID, int slaveID) {
        super(jobID, slaveID, distFile);
        this.mapper = mapper;
    }

    public Mapper getMapper() {
        return mapper;
    }

    @Override
    public String toString() {
        return super.toString() + "\n" + distFile;
    }
}
