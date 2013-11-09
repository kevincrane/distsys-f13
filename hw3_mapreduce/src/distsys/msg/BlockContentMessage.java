package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class BlockContentMessage extends Message {

    private int blockID;
    private String blockContents;

    // Simple Message for acknowledgements
    public BlockContentMessage(int blockID, String blockContents) {
        super(MessageType.BLOCK_CONTENT, blockContents);
        this.blockID = blockID;
        this.blockContents = blockContents;
    }

    public int getBlockID() {
        return blockID;
    }

    public String getBlockContents() {
        return blockContents;
    }

}
