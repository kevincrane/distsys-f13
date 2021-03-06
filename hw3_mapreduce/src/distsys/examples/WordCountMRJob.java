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
 * Canonical Example of a MapReduce Job - WORD COUNT
 * Counts the number of each type of word in the entire distributed file
 */
public class WordCountMRJob extends MapReduceJob<Integer, String, String, String> {

    public Mapper<Integer, String, String, String> getMapper() {
        return new Mapper<Integer, String, String, String>() {
            @Override
            public void map(Integer key, String value) {
                // Produces key value pairs indicating count of each token
                String[] words = value.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
                for (String keyWord : words) {
                    if (keyWord.trim().length() > 0) {
                        output.add(new Record<String, String>(keyWord.trim(), "1"));
                    }
                }
            }
        };
    }

    public Reducer<String, String, String, String> getReducer() {
        return new Reducer<String, String, String, String>() {
            @Override
            public void reduce(String key, List<String> values) {
                // Reduces values of each key by summing the counts of each token
                Integer sum = 0;
                for (String value : values) {
                    try {
                        sum += Integer.parseInt(value);
                    } catch (NumberFormatException ignored) {
                    }
                }
                output.add(new Record<String, String>(key, sum.toString()));
            }
        };
    }
}
