package distsys.kdfs;

import distsys.Config;
import distsys.msg.*;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class DataNode {

    private final String BLOCK_PREFIX = "blk_";
    private final int slaveNum;
    // BlockMap maps: blockID -> filename
    private Map<Integer, String> blockMap;
//    private CommHandler masterHandle;

    public DataNode(int slaveNum) {
        this.blockMap = new HashMap<Integer, String>();
        this.slaveNum = slaveNum;
    }

    /**
     * Read data folder where blocks are stored and generate a new BlockMap
     * BlockMap maps: blockID -> filename
     */
    public Set<Integer> generateBlockMap() {
        File blockFolder = new File(Config.BLOCK_FOLDER);
        File[] blockFiles = blockFolder.listFiles();
        Set<Integer> idSet = new HashSet<Integer>();

        if (blockFiles != null) {
            for (File f : blockFiles) {
                if (f.isFile()) {
                    // Check if file is a KDFS block file
                    String fileName = f.getName();
                    if (fileName.startsWith(BLOCK_PREFIX)) {
                        // Starts with correct prefix, add the ID number to the blockmap
                        int blockId = Integer.valueOf(fileName.substring(BLOCK_PREFIX.length()));
                        blockMap.put(blockId, f.getName());
                        idSet.add(blockId);
                    }
                }
            }
        }

        return idSet;
    }

    /**
     * Read the contents of an entire file block into a string
     * Referred from StackOverflow here: http://stackoverflow.com/questions/1656797/how-to-read-a-file-into-string-in-java
     *
     * @param filePath Name of file to read
     * @return String file contents
     * @throws IOException TODO change to use RandomAccessFile?
     */
    private String readFileAsString(String filePath) throws IOException {
        StringBuilder fileData = new StringBuilder();
        BufferedReader reader = new BufferedReader(
                new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead;
        while ((numRead = reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
        }
        reader.close();
        return fileData.toString();
    }

    /**
     * Reads a block of data from somewhere in KDFS by blockID, starting from an offset
     *
     * @param blockID ID of block to read from KDFS
     * @param offset  Start reading from a byte offset
     * @param globalFetch whether the block should only be looked for locally or if it should try to fetch it from
     *                    another slave by asking master
     * @return The contents of the block asked to read
     */
    public String readBlock(int blockID, int offset, boolean globalFetch) {
        if (blockMap.containsKey(blockID)) {
            System.out.println("Block " + blockID + " found locally.");
            // Block is stored locally, just read it and return the contents as a String
            try {
                String blockContents = readFileAsString(Config.BLOCK_FOLDER + "/" + blockMap.get(blockID));
                if (offset > 0) {
                    blockContents = blockContents.substring(offset);
                }
                return blockContents;
            } catch (IOException e) {
                System.err.println("Error: could not open DataNode file " + blockMap.get(blockID) + " (" +
                        e.getMessage() + ").");
            }
        } else if(globalFetch) {
            System.out.println("Looking elsewhere for block " + blockID);
            try {
                // Block is stored elsewhere, bleh. Ask the NameNode where it lives
                CommHandler masterHandle = new CommHandler(Config.MASTER_NODE, Config.DATA_PORT);
                masterHandle.sendMessage(new BlockAddrMessage(blockID));
                BlockAddrMessage msgIn = (BlockAddrMessage) masterHandle.receiveMessage();

                // Send message to correct DataNode asking for block contents
                if (msgIn.getSlaveNum() >= 0) {
                    int slaveNum = msgIn.getSlaveNum();
                    CommHandler dataNodeHandle = new CommHandler(Config.SLAVE_NODES[slaveNum][0], Config.SLAVE_NODES[slaveNum][1]);
                    dataNodeHandle.sendMessage(new BlockReqMessage(blockID, offset));

                    // Receive message, return block contents
                    Message blockContents = dataNodeHandle.receiveMessage();
                    return ((BlockContentMessage) blockContents).getBlockContents();
                } else {
                    System.err.println("Error: could not find block ID " + blockID + " in NameNode.");
                }
            } catch (IOException e) {
                System.err.println("Error: failed sending block request to master (" + e.getMessage() + ").");
            }
        }
        return null;
    }

    /**
     * Overloaded implementation with global reading set to true by default
     */
    public String readBlock(int blockID, int offset) {
        return readBlock(blockID, offset, true);
    }

    /**
     * Write a new block file to the local file system
     *
     * @param blockID  ID of block to write
     * @param contents Block contents
     */
    public void writeBlock(int blockID, String contents) {
        if (blockMap.containsKey(blockID)) {
            System.err.println("Error: DataNode " + slaveNum + " already contains block ID " + blockID + ".");
            return;
        }

        String fileName = String.format("%s%03d", BLOCK_PREFIX, blockID);
        System.out.println("DataNode writing block " + fileName);
        try {
            Writer outFile = new FileWriter(Config.BLOCK_FOLDER + "/" + fileName);
            outFile.write(contents);
            outFile.close();
            blockMap.put(blockID, fileName);
            System.out.println("DataNode wrote " + contents.length() + " characters to " + fileName + ".");
        } catch (IOException e) {
            System.err.println("Error: DataNode wasn't able to write to file " + fileName + " (" + e.getMessage() + ").");
        }
    }

}
