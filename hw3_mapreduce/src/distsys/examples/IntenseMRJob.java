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
 * WordCountMapReduceJob with a Thread.sleep so that it takes time on each operation for testing purposes
 */
public class IntenseMRJob extends MapReduceJob<Integer, String, String, String> {
    public static final int CHRUNCH_TIME = 10;

    public Mapper<Integer, String, String, String> getMapper() {
        return new Mapper<Integer, String, String, String>() {
            @Override
            public void map(Integer key, String value) {
                String[] words = value.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
                for (String keyWord : words) {
                    if (keyWord.trim().length() > 0) {
                        output.add(new Record<String, String>(keyWord.trim(), "1"));
                    }
                }
                try {
                    // maps are faster so make them go slower
                    Thread.sleep(CHRUNCH_TIME * 5);
                } catch (InterruptedException ignored) {
                }
            }
        };
    }

    public Reducer<String, String, String, String> getReducer() {
        return new Reducer<String, String, String, String>() {
            @Override
            public void reduce(String key, List<String> values) {
                Integer sum = 0;
                for (String value : values) {
                    try {
                        sum += Integer.parseInt(value);
                    } catch (NumberFormatException ignored) {
                    }
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
