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
    public static final int BLOCK_SIZE = 16 * 1024;     // Currently set at 16kb

    // Number of DataNodes that should store each block
    public static final int REPLICATION = 2;

    // Number of Reducers that should run during the reduce phase (more reducers = more and smaller partitions)
    public static final int NUM_REDUCERS = 3;
    public static final int MAX_TASKS_PER_NODE = 4;

    // Folder on data node to store blocks
    public static final String BLOCK_FOLDER = "data";

    // Folder on data node to store blocks
    public static final String MAP_RESULTS_FOLDER = "/tmp/mapreduce";

    // Location and file prefix of mapper result files
    public static final String MAP_RESULTS = MAP_RESULTS_FOLDER + "/mpr_";

    // Text file that logs files in the namespace for persistence
    public static final String NAMESPACE_LOG = "kdfs_namespace";

    // Maximum number of times a socket is pinged before we give up
    public static final int MAX_SOCKET_TRIES = 2;

    // Maximum number of times a job is retried on a different slave before we give up on the entire mapreduce job
    public static final int MAX_JOB_RETRIES = 2;

    // Specifies whether all blocks on a dead slave should be replicated once in another slave to maintain the replication factor
    public static final boolean REPLICATE_BLOCKS_ON_SLAVE_DOWN = true;

}
