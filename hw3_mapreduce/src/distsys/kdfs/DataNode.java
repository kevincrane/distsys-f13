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

    public DataNode(int slaveNu) {
        this.blockMap = new HashMap<Integer, String>();
        this.slaveNum = slaveNu;

        // Automatically ping Master/NameNode every 2 seconds with current blockmap
        Timer pingTimer = new Timer();
        pingTimer.schedule(new TimerTask() {
            public void run() {
                try {
                    CommHandler timedHandle = new CommHandler(Config.MASTER_NODE, Config.DATA_PORT);
                    timedHandle.sendMessage(new BlockMapMessage(slaveNum, generateBlockMap()));
                } catch (IOException e) {
                    System.err.println("Error: could not send BlockMap to master node (" + e.getMessage() + ").");
                }
            }
        }, 5000, 5000);
    }

    /**
     * Read data folder where blocks are stored and generate a new BlockMap
     * BlockMap maps: blockID -> filename
     */
    public Set<Integer> generateBlockMap() {
        File blockFolder = new File(Config.BLOCK_FOLDER);
        File[] blockFiles = blockFolder.listFiles();
        Set<Integer> idSet = new HashSet<Integer>();

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

        return idSet;
    }

    /**
     * Reads a block of data from somewhere in KDFS by blockID, starting from an offset
     *
     * @param blockID ID of block to read from KDFS
     * @param offset  Start reading from a byte offset     //TODO: come back to add offset stuff
     * @return The contents of the block asked to read
     */
    public String readBlock(int blockID, long offset) {
        if (blockMap.containsKey(blockID)) {
            System.out.println("Block " + blockID + " found locally.");
            // Block is stored locally, just read it and return the contents as a String
            try {
                //TODO: add offset here
                File blockFile = new File(Config.BLOCK_FOLDER + "/" + blockMap.get(blockID));
                String blockContents = new Scanner(blockFile).useDelimiter("\\Z").next();
                return blockContents;
            } catch (FileNotFoundException e) {
                System.err.println("Error: could not open DataNode file " + blockMap.get(blockID) + " (" +
                        e.getMessage() + ").");
            }
        } else {
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
