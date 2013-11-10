package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class BlockReqMessage extends Message {

    private int blockID;
    private int offset;

    // Request a block address from NameNode
    public BlockReqMessage(int blockID) {
        super(MessageType.BLOCK_REQ, blockID);
        this.blockID = blockID;
        this.offset = 0;
    }

    public BlockReqMessage(int blockID, int offset) {
        super(MessageType.BLOCK_REQ, blockID);
        this.blockID = blockID;
        this.offset = offset;
    }

    public int getBlockId() {
        return blockID;
    }

    public int getOffset() {
        return offset;
    }

}
