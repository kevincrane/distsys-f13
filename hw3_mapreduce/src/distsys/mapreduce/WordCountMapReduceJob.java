package distsys.mapreduce;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/12/13
 */

/**
 * Canonical Example of a MapReduce Job - WORD COUNT
 * Counts the number of each type of word in the entire distributed file
 *
 */
public class WordCountMapReduceJob extends MapReduceJob<Integer, String, String, Integer> {

    public Mapper<Integer, String, String, Integer> getMapper() {
        return new Mapper<Integer, String, String, Integer>() {
            @Override
            public void map(Integer key, String value) {
                // Default map method just produces an identity mapper
                String[] words = value.trim().split(" ");
                for (String word: words) {
                    output.add(new Record<String, Integer>(word, 1));
                }
            }
        };
    }

    public Reducer<String, Integer, String, Integer> getReducer() {
        return new Reducer<String, Integer, String, Integer>() {
            @Override
            public void reduce(String key, List<Integer> values) {
                // Default reduce method just produces an identity reducer
                int sum = 0;
                for (Integer value : values) {
                    sum += value;
                }
                output.add(new Record<String, Integer>(key, sum));
            }
        };
    }
}
