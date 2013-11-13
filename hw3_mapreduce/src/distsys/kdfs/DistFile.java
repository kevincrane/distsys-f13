package distsys.kdfs;

import distsys.Config;
import distsys.mapreduce.Record;
import distsys.msg.BlockPosMessage;
import distsys.msg.CommHandler;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 *
 * Abstraction representing a distributed file that allows the user to read records from the DataNode
 * which is set by the slave to it's own DataNode
 *
 */
public class DistFile implements Serializable {

    private DataNode dataNode;
    private String fileName;
    private int position;
    private int endPosition;
    private String buffer;
    private int bufferStart;

    public DistFile(String fileName, int startPosition, int endPosition) {
        this.fileName = fileName;

        // Set up starting position
        this.position = startPosition;
        this.endPosition = endPosition;
    }

    public int getPosition() {
        return position;
    }

    public void setDataNode(DataNode dataNode) {
        this.dataNode = dataNode;
        buffer = "";
        int startPosition = position;
        bufferStart = startPosition;

        // If starting buffer from later block, check if you need to skip the first record (leftover from prev. block)
        if (startPosition > 0) {
            startPosition--;
            bufferStart--;
            seek(startPosition);
        } else {
            seek(startPosition);
        }
    }

    @Override
    public String toString() {
        return "DistFile: {dataNode: " + dataNode + ", fileName: " + fileName + ", position: " + position + ", endposition: " + endPosition + "}";
    }


    /**
     * Seek file to new character position
     *
     * @param position Position in file to seek to
     */
    public void seek(int position) {
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
                } else {
                    // Could not find valid block, must not exist
                    this.position = -1;
                }
            } catch (IOException e) {
                System.err.println("Error: error sending block position request to NameNode.");
            }
        } else {
            // We already have what we need in the buffer
            this.position = position;
        }
    }

    /**
     * Read a mapreduce record from the DistFile.
     * Delimited by newline.
     * Extends into next blocks if needed
     *
     * @return Next String record from DistFile
     */
    public Record<Integer, String> nextRecord() {
        // Make sure you haven't extended past the end of the block assignment
        if (position >= endPosition) {
            return null;
        }

        // Set the Record key as the position offset of the file being read
        int key = position;

        int recordEnd = buffer.indexOf('\n', position - bufferStart);
        String record = "";

        // If this record extends past the end of a block, seek to a new block to continue
        while (recordEnd == -1 && position >= 0) {
            // Add remainder of the buffer to string and seek to next block
            record += buffer.substring(position - bufferStart);
            position = bufferStart + buffer.length();
            seek(position);
            recordEnd = buffer.indexOf('\n', position - bufferStart);
        }

        // Add remainder of the buffer read to the record
        if (position >= 0) {
            record += buffer.substring(position - bufferStart, recordEnd);
            position += record.length() + 1;
        }

        // If you haven't read anything and you've finished the block or record, end
        if (record.length() == 0 && (recordEnd < 0 || position < 0)) {
            return null;
        }

        return new Record<Integer, String>(key, record);
    }

}