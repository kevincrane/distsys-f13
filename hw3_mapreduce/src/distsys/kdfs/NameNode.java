package distsys.kdfs;

import distsys.Config;
import distsys.msg.BlockAddrMessage;
import distsys.msg.BlockMapMessage;
import distsys.msg.CommHandler;
import distsys.msg.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class NameNode {

    // Map of datanode hostnames to block IDs they carry
    private Map<Integer, Set<Integer>> blockMap;

    // Map of file names to block IDs
    private Map<String, List<Integer>> namespace;

    // Metadata about each KDFS block
    private Map<Integer, BlockInfo> blockData;


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
    public void pingSlaves() {
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

        do {
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

                // Add BlockInfo to map of ID -> Info and to list of blocks associated with a filename
                spaceIds.add(Integer.parseInt(blockInfo[0]));
                blockData.put(blockID, new BlockInfo(blockID, fileInfo[0], offset, length));
                blockCount++;
            }

            // Add this set of ids to the namespace in memory
            namespace.put(fileInfo[0].trim(), spaceIds);
        } while (namespaceScanner.hasNextLine());
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
                } catch (IOException e) {
                    System.err.println("Error: error sending block address to DataNode.");
                }
                return;
            }
        }

        // Failed, send a sad message instead
        try {
            dataNodeHandle.sendMessage(new BlockAddrMessage(-1, blockID));
        } catch (IOException e) {
            System.err.println("Error: error sending block address to DataNode.");
        }
    }


}
