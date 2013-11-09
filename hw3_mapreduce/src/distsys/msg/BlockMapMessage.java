package distsys.msg;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class BlockMapMessage extends Message {

    private int hostnum;
    private Set<Integer> blocks;

    // Simple Message for acknowledgements
    public BlockMapMessage(int hostNum, Set<Integer> blocks) {
        super(MessageType.BLOCKMAP, blocks);
        this.hostnum = hostNum;
        this.blocks = blocks;
    }

    public int getHostnum() {
        return hostnum;
    }

    public Set<Integer> getBlocks() {
        return blocks;
    }

}
