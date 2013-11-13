package distsys.mapreduce;

import distsys.kdfs.DataNode;
import distsys.kdfs.DistFile;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 */

public abstract class FileTask extends Task {
    private DistFile distFile;

    protected FileTask(int jobID, int slaveID, DistFile distFile) {
        super(jobID, slaveID);
        this.distFile = distFile;
    }

    public void setDataNode(DataNode dataNode) {
        distFile.setDataNode(dataNode);
    }

    public DistFile getDistFile() {
        return distFile;
    }
}