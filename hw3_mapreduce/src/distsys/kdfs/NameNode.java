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
public class NameNode {

    // Map of datanode host IDs to block IDs they carry
    private Map<Integer, Set<Integer>> blockMap;

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
    private void pingSlaves() {
        blockMap = new HashMap<Integer, Set<Integer>>();

        // Connect to all known slaves and ask for their BlockMap
        for (int i = 0; i < Config.SLAVE_NODES.length; i++) {
            String[] slave = Config.SLAVE_NODES[i];

            if (slave.length != 2) {
                //TODO: handle not having a port in config?
                continue;
            }
            try {
                // Create connection with slave
                CommHandler slaveHandle = new CommHandler(slave[0], slave[1]);

                // Request BlockMap from slave
                slaveHandle.sendMessage(new BlockMapMessage(i, null));
                Message returnedMsg = slaveHandle.receiveMessage();

                // Received Blockmap, adding its contents to memory
                if (returnedMsg instanceof BlockMapMessage) {
                    blockMap.put(i, ((BlockMapMessage) returnedMsg).getBlocks());
                }
            } catch (IOException ignored) {
            }
        }
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
                long offset = Long.parseLong(blockInfo[1]);
                long length = Long.parseLong(blockInfo[2]);
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
        // Randomize order of nodes to check for even distribution of requests
        List<Integer> dataNodes = new ArrayList<Integer>();
        dataNodes.addAll(blockMap.keySet());
        Collections.shuffle(dataNodes);

        // Check to see if each node in the block map contains this ID
        for (Integer slaveNum : dataNodes) {
            if (blockMap.get(slaveNum).contains(blockID)) {
                // Found a node, send a message with the socket
                try {
                    dataNodeHandle.sendMessage(new BlockAddrMessage(slaveNum, blockID));
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
     * Write a new file to the KDFS filesystem
     *
     * @param fileName Filename of the file to write
     */
    public void putFile(String fileName) {
        if (namespace.containsKey(fileName)) {
            System.err.println("Error: KDFS namespace already contains file " + fileName + ".");
            return;
        }

        String fileContents;
        try {
            fileContents = new Scanner(new File(fileName)).useDelimiter("\\Z").next();
        } catch (FileNotFoundException e) {
            System.err.println("Error: NameNode could not open file " + fileName + " (" + e.getMessage() + ").");
            return;
        }

        // Randomize order of nodes to check for even distribution of requests
        List<Integer> dataNodes = new ArrayList<Integer>();
        dataNodes.addAll(blockMap.keySet());
        Collections.shuffle(dataNodes);

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
            for (int rep = 0; rep < Config.REPLICATION && rep < dataNodes.size(); rep++) {
                // Make sure slave doesn't contain this block already
                int currentSlave = dataNodes.get(nextSlaveIdx);
                if (blockMap.get(currentSlave).contains(maxBlockID)) {
                    System.out.println("breakin'");
                    break;
                }

                System.out.println("Making Slave " + currentSlave + " write block starting from pos " + pos);
                try {
                    // Send Block write request to next DataNode
                    CommHandler writingSlave = new CommHandler(Config.SLAVE_NODES[currentSlave][0],
                            Config.SLAVE_NODES[currentSlave][1]);
                    writingSlave.sendMessage(new BlockContentMessage(maxBlockID, currentBlock));
                    // TODO: if you need acknowledgement from DataNode, it would go here

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

                nextSlaveIdx = (nextSlaveIdx + 1) % dataNodes.size();
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
            for (int slaveID : blockMap.keySet()) {
                if (blockMap.get(slaveID).contains(blockID)) {
                    // Found a DataNode with this block, try to get it
                    try {
                        CommHandler blockReadHandle = new CommHandler(Config.SLAVE_NODES[slaveID][0],
                                Config.SLAVE_NODES[slaveID][1]);
                        blockReadHandle.sendMessage(new BlockReqMessage(blockID));
                        BlockContentMessage contentMsg = (BlockContentMessage) blockReadHandle.receiveMessage();

                        if (contentMsg.getBlockContents() == null) {
                            System.err.println("Error: DataNode " + slaveID + " returned empty content, trying new node.");
                            continue;
                        }

                        // Append contents and move on to next Block ID
                        fileContents.append(contentMsg.getBlockContents());
                        break;
                    } catch (IOException e) {
                        System.err.println("Error: could not connect to DataNode " + slaveID + " (" + e.getMessage() + ").");
                    }
                }
            }
        }

        return fileContents.toString();
    }

    /**
     * Print out a list of all files in the namespace
     */
    public void listFiles() {
        List<String> sortedFileNames = new ArrayList<String>(namespace.keySet());
        Collections.sort(sortedFileNames);

        System.out.println("KDFS namespace contains:");
        for (String filename : sortedFileNames) {
            System.out.println(filename + " : " + namespace.get(filename).size() + " blocks");
        }
    }

}
