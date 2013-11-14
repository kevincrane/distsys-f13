package distsys.examples;

import distsys.mapreduce.MapReduceJob;
import distsys.mapreduce.Mapper;
import distsys.mapreduce.Record;
import distsys.mapreduce.Reducer;

import java.util.List;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/12/13
 */

/**
 * Canonical Example of a MapReduce Job - WORD COUNT
 * Counts the number of each type of word in the entire distributed file
 */
public class IntenseMapReduceJob extends MapReduceJob<Integer, String, String, String> {
    public static final int CHRUNCH_TIME = 10000;

    public Mapper<Integer, String, String, String> getMapper() {
        return new Mapper<Integer, String, String, String>() {
            @Override
            public void map(Integer key, String value) {
                // Default map method just produces an identity mapper
                StringTokenizer tokens = new StringTokenizer(value.trim());
                while (tokens.hasMoreTokens()) {
                    output.add(new Record<String, String>(tokens.nextToken(), "1"));
                }
                try {
                    Thread.sleep(CHRUNCH_TIME);
                } catch (InterruptedException ignored) {
                }
            }
        };
    }

    public Reducer<String, String, String, String> getReducer() {
        return new Reducer<String, String, String, String>() {
            @Override
            public void reduce(String key, List<String> values) {
                // Default reduce method just produces an identity reducer
                Integer sum = 0;
                for (String value : values) {
                    sum += Integer.parseInt(value);
                }
                output.add(new Record<String, String>(key, sum.toString()));
                try {
                    Thread.sleep(CHRUNCH_TIME);
                } catch (InterruptedException ignored) {
                }
            }
        };
    }
}
