package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class BlockAddrMessage extends Message {

    private int slaveNum;
    private int blockID;

    // Request a block address from NameNode
    public BlockAddrMessage(int blockID) {
        super(MessageType.BLOCK_ADDR, blockID);
        this.blockID = blockID;
    }

    public BlockAddrMessage(int slaveNum, int blockID) {
        super(MessageType.BLOCK_ADDR, blockID);
        this.slaveNum = slaveNum;
        this.blockID = blockID;
    }

    public int getBlockId() {
        return blockID;
    }

    public int getSlaveNum() {
        return slaveNum;
    }

}
