package distsys.kdfs;

import distsys.Config;
import distsys.msg.BlockPosMessage;
import distsys.msg.CommHandler;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class DistFile {

    private DataNode dataNode;
    private String fileName;
    private long position;
    private String buffer;
    private long bufferStart;

    public DistFile(DataNode dataNode, String fileName, long startingPosition) {
        this.dataNode = dataNode;
        this.fileName = fileName;

        this.position = startingPosition;
        buffer = "";
        bufferStart = startingPosition;
        seek(startingPosition);
    }


    /**
     * Seek file to new character position
     *
     * @param position Position in file to seek to
     */
    public void seek(long position) {
        if (position < bufferStart || position >= bufferStart + buffer.length()) {
            // Position is not stored in buffer, load new block
            try {
                // Send request to NameNode for blockId containing this position
                CommHandler nameNodeHandle = new CommHandler(Config.MASTER_NODE, Config.DATA_PORT);
                nameNodeHandle.sendMessage(new BlockPosMessage(fileName, 0, position));
                BlockPosMessage blockPosMessage = (BlockPosMessage) nameNodeHandle.receiveMessage();

                System.out.println("Looking for position " + position + " in block " + blockPosMessage.getBlockId());

                // Ask DataNode for contents of block starting from position
                if (blockPosMessage.getBlockId() >= 0) {
                    buffer = dataNode.readBlock(blockPosMessage.getBlockId(), position - blockPosMessage.getBlockStart());
                    this.position = position;
                    this.bufferStart = position;
                    System.out.println("Seeked to position " + this.position + "; buffer contains:\n" + buffer);
                }
            } catch (IOException e) {
                System.err.println("Error: error sending block position request to NameNode.");
            }
        } else {
            // We already have what we need in the buffer
            this.position = position;
            System.out.println("Already have buffer. Seeked to position " + this.position + "; buffer contains:\n" + buffer);
        }
    }

}
