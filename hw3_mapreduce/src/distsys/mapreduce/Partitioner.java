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
    public int getPartition(Object key, int numReducers) {
        return key.hashCode() % numReducers;
    }
}
