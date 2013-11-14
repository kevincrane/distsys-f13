package distsys.mapreduce;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/12/13
 */
public class Partitioner {

    /**
     * Hashes the key and returns which partition the key should belong
     *
     * @param key         The Record key to be hashed
     * @param numReducers The number of Reducers to consider
     * @return Which partition/reducer the key should be sent to
     */
    public static int getPartition(String key, int numReducers) {
        int partition = key.hashCode() % numReducers;
        if (partition < 0) {
            partition *= -1;
        }
        return partition;
    }
}
