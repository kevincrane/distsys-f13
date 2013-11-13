package distsys.mapreduce;

import distsys.kdfs.DataNode;
import distsys.kdfs.DistFile;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 * Time: 1:37 AM
 * To change this template use File | Settings | File Templates.
 */

public abstract class FileTask extends Task {
    protected DistFile distFile;

    protected FileTask(int jobID, int slaveID, DistFile distFile) {
        super(jobID, slaveID);
        this.distFile = distFile;
    }

    public void setDataNode(DataNode dataNode) {
        distFile.setDataNode(dataNode);
    }
}