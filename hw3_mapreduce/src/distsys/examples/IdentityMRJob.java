package distsys.examples;

import distsys.mapreduce.MapReduceJob;
import distsys.mapreduce.Mapper;
import distsys.mapreduce.Record;
import distsys.mapreduce.Reducer;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/12/13
 */

/**
 * Canonical Example of a MapReduce Job - IDENTITY
 * Returns the same document that is loaded with the character offset as a key
 */
public class IdentityMRJob extends MapReduceJob<Integer, String, String, String> {

    public Mapper<Integer, String, String, String> getMapper() {
        return new Mapper<Integer, String, String, String>() {
            @Override
            public void map(Integer key, String value) {
                // Default map method just produces an identity mapper
                output.add(new Record<String, String>(key.toString(), value));
            }
        };
    }

    public Reducer<String, String, String, String> getReducer() {
        return new Reducer<String, String, String, String>() {
            @Override
            public void reduce(String key, List<String> values) {
                // Default reduce method just produces an identity reducer
                for (String value : values) {
                    output.add(new Record<String, String>(key, value));
                }
            }
        };
    }
}
