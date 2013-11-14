package distsys.msg;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class BlockAddrMessage extends Message {

    private List<Integer> slaveIds;
    private int blockID;

    // Request a block address from NameNode
    public BlockAddrMessage(int blockID) {
        super(MessageType.BLOCK_ADDR, blockID);
        this.blockID = blockID;
    }

    public BlockAddrMessage(List<Integer> slaveIds, int blockID) {
        super(MessageType.BLOCK_ADDR, blockID);
        this.slaveIds = slaveIds;
        this.blockID = blockID;
    }

    public int getBlockId() {
        return blockID;
    }

    public List<Integer> getSlaveIds() {
        return slaveIds;
    }

}
