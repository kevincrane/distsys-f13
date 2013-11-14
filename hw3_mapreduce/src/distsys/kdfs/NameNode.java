package distsys.kdfs;

import distsys.Config;
import distsys.MasterNode;
import distsys.msg.*;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */

/**
 * Responsible for maintaing list of block addresses and record of living slaves.
 * Can potentially be run on an instance that is seperate from the master Node
 */
public class NameNode {

    // Map of datanode host IDs to block IDs they carry
    public Map<Integer, Set<Integer>> blockMap;

    // Map of file names to block IDs
    private Map<String, List<Integer>> namespace;

    // Metadata about each KDFS block
    private Map<Integer, BlockInfo> blockData;

    // Highest block ID currently in KDFS
    private int maxBlockID = 0;

    /**
     * Default Constructor
     */
    public NameNode() {
        blockMap = new HashMap<Integer, Set<Integer>>();
        namespace = new HashMap<String, List<Integer>>();
        blockData = new HashMap<Integer, BlockInfo>();

        // Load a persistent copy of the namespace to memory
        loadNamespace(Config.NAMESPACE_LOG);
        pingSlaves();
    }


    /**
     * Initialization of NameNode; load Namespace from log file and ask every known slave for its BlockMap
     */
    public List<Integer> pingSlaves() {
        HashMap<Integer, Set<Integer>> newblockMap = new HashMap<Integer, Set<Integer>>();
        List<Integer> deadSlaveIds = new ArrayList<Integer>();

        // Connect to all known slaves and ask for their BlockMap
        for (int i = 0; i < Config.SLAVE_NODES.length; i++) {
            String[] slave = Config.SLAVE_NODES[i];

            try {
                // Create connection with slave
                CommHandler slaveHandle = new CommHandler(slave[0], slave[1]);

                // Request BlockMap from slave
                slaveHandle.sendMessage(new BlockMapMessage(i, null));
                Message returnedMsg = slaveHandle.receiveMessage();

                // Received Blockmap, adding its contents to memory
                if (returnedMsg instanceof BlockMapMessage) {
                    newblockMap.put(i, ((BlockMapMessage) returnedMsg).getBlocks());
                }
            } catch (IOException ignored) {
                // tell master that a slave failed
                deadSlaveIds.add(i);
                //TODO Handle TimeoutException if slave doesn't reply and is down
                //TODO IMPORTANT IMPORTANT handle REMOVAL of slave if slave is down and doesn't reply - need different AWAKE message instead of generating blockMap on Slave every 5 seconds

                //TODO IMP: IF slave down tell CoOrdinator that slave is down and so it can assign the job to some other slave with maxTries of 3
            }
        }

        blockMap = newblockMap;
        return deadSlaveIds;
    }


    /**
     * Load the NameNode's namespace from the log file on the Master node
     * Format: filename (\t) id, offset, len (\t) id, offset, len
     * Example: hello.txt   1,0,64  2,64,64 3,128,64
     *
     * @param namespaceFileName Name of the file where the namespace info is listed
     */
    private void loadNamespace(String namespaceFileName) {
        Scanner namespaceScanner;
        int blockCount = 0;
        try {
            // Open the namespace file on the local disk
            namespaceScanner = new Scanner(new File(namespaceFileName));
        } catch (FileNotFoundException e) {
            System.err.println("Error: could not load namespace from file '" + namespaceFileName + "'. Namespace is empty.");
            return;
        }

        while (namespaceScanner.hasNextLine()) {
            // Read a line from the namespace file
            String[] fileInfo = namespaceScanner.nextLine().split("\t");
            if (fileInfo.length < 2) {
                continue;
            }

            // Iterate through each block info found (id, offset, len)
            List<Integer> spaceIds = new ArrayList<Integer>();
            for (int i = 1; i < fileInfo.length; i++) {
                String[] blockInfo = fileInfo[i].split(",");

                if (blockInfo.length != 3) continue;     // Invalid block info format

                // Extract block params for this particular block
                int blockID = Integer.parseInt(blockInfo[0]);
                int offset = Integer.parseInt(blockInfo[1]);
                int length = Integer.parseInt(blockInfo[2]);
                if (blockID > maxBlockID) {
                    maxBlockID = blockID;
                }

                // Add BlockInfo to map of ID -> Info and to list of blocks associated with a filename
                spaceIds.add(Integer.parseInt(blockInfo[0]));
                blockData.put(blockID, new BlockInfo(blockID, fileInfo[0], offset, length));
                blockCount++;
            }

            // Add this set of ids to the namespace in memory
            namespace.put(fileInfo[0].trim(), spaceIds);
        }
        namespaceScanner.close();
        System.out.println("NameNode: Loaded " + blockCount + " blocks from " + namespace.size() + " filenames into namespace.");
    }

    /**
     * Update stored block map for one Slave host
     *
     * @param slaveNum Slave host CommHandler
     * @param blocks   Set of block IDs
     */
    public void updateBlockMap(int slaveNum, Set<Integer> blocks) {
        blockMap.put(slaveNum, blocks);
    }

    /**
     * Send a CommHandle of a node containing blockID so DataNode can retrieve it
     *
     * @param dataNodeHandle Handle to DataNode
     * @param blockID        ID of block to retrieve
     */
    public void retrieveBlockAddress(CommHandler dataNodeHandle, int blockID) {
        // Check to see if each node in the block map contains this ID
        for (Integer slaveId : getSlaveIds()) {
            if (blockMap.get(slaveId).contains(blockID)) {
                // Found a node, send a message with the socket
                try {
                    dataNodeHandle.sendMessage(new BlockAddrMessage(slaveId, blockID));
                    return;
                } catch (IOException e) {
                    System.err.println("Error: error sending block address to DataNode.");
                }
            }
        }

        // Failed, send a sad message instead
        try {
            dataNodeHandle.sendMessage(new BlockAddrMessage(-1, blockID));
        } catch (IOException e) {
            System.err.println("Error: error sending block address to DataNode.");
        }
    }

    /**
     * Randomize order of nodes to check for even distribution of requests
     *
     * @return List of DataNode Ids in randomized order
     */
    public List<Integer> getSlaveIds() {
        List<Integer> dataNodes = new ArrayList<Integer>();
        dataNodes.addAll(blockMap.keySet());
        Collections.shuffle(dataNodes);
        return dataNodes;
    }

    /**
     * Write a new file to the KDFS filesystem
     *
     * @param fileName Filename of the file to write
     * @return The number of blocks written to the file system
     */
    public int putFile(String fileName) {
        if (namespace.containsKey(fileName)) {
            System.err.println("Error: KDFS namespace already contains file " + fileName + ".");
            return 0;
        }

        String fileContents;
        try {
            fileContents = new Scanner(new File(fileName)).useDelimiter("\\Z").next();
        } catch (FileNotFoundException e) {
            System.err.println("Error: NameNode could not open file " + fileName + " (" + e.getMessage() + ").");
            return 0;
        }

        List<Integer> slaveIds = getSlaveIds();
        int pos = 0;
        int nextSlaveIdx = 0;
        List<Integer> blocksWritten = new ArrayList<Integer>();
        String namespaceEntry = fileName;

        // Split file into blocks and write to DataNodes
        while (pos < fileContents.length()) {
            maxBlockID++;       // Everything ready, prep ID number

            // Set index to cut off block end at
            int endPos = pos + Config.BLOCK_SIZE;
            if (endPos > fileContents.length()) {
                endPos = fileContents.length();
            }

            // Separate out contents of this block
            String currentBlock = fileContents.substring(pos, endPos);

            // Write each block rep times, up to the Replication Factor
            for (int rep = 0; rep < Config.REPLICATION && rep < slaveIds.size(); rep++) {
                // Make sure slave doesn't contain this block already
                int currentSlave = slaveIds.get(nextSlaveIdx);
                if (blockMap.get(currentSlave).contains(maxBlockID)) {
                    break;
                }

                System.out.println("Making Slave " + currentSlave + " write block starting from pos " + pos);
                try {
                    // Send Block write request to next DataNode
                    CommHandler writingSlave = new CommHandler(Config.SLAVE_NODES[currentSlave][0],
                            Config.SLAVE_NODES[currentSlave][1]);
                    writingSlave.sendMessage(new BlockContentMessage(maxBlockID, currentBlock));
                    // TODO: NEED THIS FOR FAULT TOLERANCE, IF NODE HASN'T WRITTEN OR FAILED - TRY NEXT SLAVE TILL WE RUN OUT
                    // - also if we implement this we need a way to ensure replicas don't go the same slave

                    // Update BlockMap on success
                    Set<Integer> slaveBlocks = blockMap.get(currentSlave);
                    if (slaveBlocks == null) {
                        slaveBlocks = new HashSet<Integer>();
                    }
                    slaveBlocks.add(maxBlockID);
                    blockMap.put(currentSlave, slaveBlocks);
                } catch (IOException e) {
                    System.err.println("Error: NameNode could not send Block write request to " +
                            Config.SLAVE_NODES[currentSlave][0] + ":" + Config.SLAVE_NODES[currentSlave][1]);
                }

                nextSlaveIdx = (nextSlaveIdx + 1) % slaveIds.size();
            }

            // Update metadata in NameNode
            blocksWritten.add(maxBlockID);
            blockData.put(maxBlockID, new BlockInfo(maxBlockID, fileName, pos, endPos - pos));
            namespaceEntry += "\t" + maxBlockID + "," + pos + "," + (endPos - pos);
            pos = endPos;
        }

        // Update namespace
        if (blocksWritten.size() > 0) {
            namespace.put(fileName, blocksWritten);
            try {
                Writer namespaceFile = new BufferedWriter(new FileWriter(Config.NAMESPACE_LOG, true));
                namespaceEntry += "\n";
                namespaceFile.append(namespaceEntry);
                namespaceFile.close();
            } catch (IOException e) {
                System.err.println("Error: failed to update namespace log file.");
            }
        }

        return blocksWritten.size();
    }

    /**
     * Read a file from KDFS and return its contents as as tring
     *
     * @param fileName File's name in the namespace
     * @return Contents of the file
     */
    public String readFile(String fileName) {
        List<Integer> blockIDs = namespace.get(fileName);
        if (blockIDs == null) {
            System.err.println("Error: KDFS namespace does not contain file " + fileName + ".");
            return null;
        }

        // Iterate through each block ID for this filename
        StringBuilder fileContents = new StringBuilder();
        for (int blockID : blockIDs) {
            // Check each DataNode to see if they have this block
            String contentsToAdd = null;
            // Randomize order to prevent one slave from getting more of the requests
            List<Integer> slaveIDs = getSlaveIds();
            for (int slaveID : slaveIDs) {
                if (blockMap.get(slaveID).contains(blockID)) {
                    try {
                        // Found a DataNode with this block, try to get it
                        CommHandler blockReadHandle = new CommHandler(Config.SLAVE_NODES[slaveID][0],
                                Config.SLAVE_NODES[slaveID][1]);
                        blockReadHandle.sendMessage(new BlockReqMessage(blockID));
                        BlockContentMessage contentMsg = (BlockContentMessage) blockReadHandle.receiveMessage();

                        // Verify contents of block exist
                        if (contentMsg.getBlockContents() == null) {
                            System.err.println("Error: DataNode " + slaveID + " returned empty content, trying new node.");
                            continue;
                        }

                        // Read contents of this block
                        contentsToAdd = contentMsg.getBlockContents();
                        break;
                    } catch (IOException e) {
                        System.err.println("Error: could not connect to DataNode " + slaveID + " (" + e.getMessage() + ").");
                    }
                }
            }
            // Append contents if they've been found and move on to next Block ID
            if (contentsToAdd == null) {
                System.err.println("Error: could not find block " + blockID + " in KDFS.");
                return null;
            }
            fileContents.append(contentsToAdd);
        }

        return fileContents.toString();
    }

    /**
     * Return the names of all of the files stored in the namespace
     */
    public Set<String> listFiles() {
        return namespace.keySet();
    }

    /**
     * Get the block IDs of a file by name
     *
     * @param fileName Name of file to retrieve blocks
     * @return A list of block IDs that correspond to a particular filename
     */
    public List<Integer> getFileBlockIds(String fileName) {
        return namespace.get(fileName);
    }

    /**
     * Get the Block MetaData assocaited with a particular blockId
     *
     * @param blockId ID for the block whose data you wish to receive
     * @return BlockInfo that corresponds to a particular blockId
     */
    public BlockInfo getBlockInfo(int blockId) {
        return blockData.get(blockId);
    }

    /**
     * Return a Block ID that contains a given position to seek to
     *
     * @param fileName Name of file being read
     * @param position Position to seek in file
     */
    public void returnBlockIdByPosition(CommHandler dataNodeHandle, String fileName, int position) {
        List<Integer> fileBlocks = namespace.get(fileName);

        if (fileBlocks != null) {
            // Iterate through each block that is listed for this file in the namespace
            for (int blockId : fileBlocks) {
                BlockInfo blockInfo = blockData.get(blockId);
                if (blockInfo.containsPosition(position)) {
                    // Found a block that contains this position, send message back to slave
                    try {
//                        System.out.println("Found " + fileName + " position " + position + " in block " + blockId);
                        dataNodeHandle.sendMessage(new BlockPosMessage(fileName, blockId, blockInfo.getOffset()));
                        return;
                    } catch (IOException e) {
                        System.err.println("Error: error sending block address to DataNode.");
                    }
                }
            }
        }

//        System.err.println("Error: NameNode couldn't find file " + fileName + " to seek to for position " + position + ".");
        try {
            dataNodeHandle.sendMessage(new BlockPosMessage(fileName, -1, 0));
        } catch (IOException e) {
            System.err.println("Error: error sending block address to DataNode.");
        }
    }

    /**
     * Return a list of slave IDs that contain a block with given position in a file
     *
     * @param fileName Name of the file to look in
     * @param position Where in the file we want
     * @return List of block IDs
     */
    public List<Integer> getSlaveIdsFromPosition(String fileName, int position) {
        List<Integer> fileBlocks = namespace.get(fileName);
        List<Integer> matchingSlaveIDs = new ArrayList<Integer>();
        int matchingBlock = -1;

        // Iterate through each block that is listed for this file in the namespace
        for (int blockId : fileBlocks) {
            BlockInfo blockInfo = blockData.get(blockId);
            if (blockInfo.containsPosition(position)) {
                // Found a block that contains this position,
                matchingBlock = blockId;
            }
        }

        // For each living slave, check if it contains this block ID and add to list of matching slaves
        if (matchingBlock >= 0) {
            for (Integer slaveId : blockMap.keySet()) {
                if (blockMap.get(slaveId).contains(matchingBlock)) {
                    matchingSlaveIDs.add(slaveId);
                }
            }
        }

        return matchingSlaveIDs;
    }

}
