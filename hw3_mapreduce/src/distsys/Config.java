package distsys;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class Config {

    // Master and slave node locations
    public static final String MASTER_NODE = "localhost";
    public static final String[][] SLAVE_NODES = {
            {"localhost", "12345"},
            {"localhost", "12346"},
            {"localhost", "12347"}
    };

    // Port used for transferring data between nodes
    public static final int DATA_PORT = 14087;

    // KDFS block size (in bytes)
    public static final int BLOCK_SIZE = 50; // 1 * 1024 * 1024;

    // Number of DataNodes that should store each block
    public static final int REPLICATION = 4;

    // Folder on data node to store blocks
    public static final String BLOCK_FOLDER = "data";

    // Text file that logs files in the namespace for persistence
    public static final String NAMESPACE_LOG = "kdfs_namespace";

}
