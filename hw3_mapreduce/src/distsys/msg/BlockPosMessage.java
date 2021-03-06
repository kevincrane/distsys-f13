package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class BlockPosMessage extends Message {

    private String fileName;
    private int blockID;
    private int blockStart;

    // Request a block address from NameNode
    public BlockPosMessage(String fileName, int blockID, int blockStart) {
        super(MessageType.BLOCK_POS, blockID);
        this.fileName = fileName;
        this.blockID = blockID;
        this.blockStart = blockStart;
    }

    public int getBlockId() {
        return blockID;
    }

    public int getBlockStart() {
        return blockStart;
    }

    public String getFileName() {
        return fileName;
    }

}
