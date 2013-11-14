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
 * Compute statistics about the document that is being read
 * Computes average word length, number of words, and number of lines
 */
public class DocStatMRJob extends MapReduceJob<Integer, String, String, String> {

    public Mapper<Integer, String, String, String> getMapper() {
        return new Mapper<Integer, String, String, String>() {
            @Override
            public void map(Integer key, String value) {
                // Add values for number of lines, words, and letters
                String[] words = value.trim().split(" ");
                output.add(new Record<String, String>("num_lines", "1"));
                output.add(new Record<String, String>("num_words", "" + words.length));

                for (String word : words) {
                    output.add(new Record<String, String>("avg_word_len", "" + word.trim().length()));
                }
            }
        };
    }

    public Reducer<String, String, String, String> getReducer() {
        return new Reducer<String, String, String, String>() {
            @Override
            public void reduce(String key, List<String> values) {
                // Figure out which op and calculate result
                if (key.equals("num_lines")) {
                    // Number of lines in the document
                    output.add(new Record<String, String>(key, "" + values.size()));
                } else if (key.equals("num_words")) {
                    // Number of words in the document
                    int sum = 0;
                    for (String numWords : values) {
                        sum += Integer.parseInt(numWords);
                    }
                    output.add(new Record<String, String>(key, "" + sum));
                } else if (key.equals("avg_word_len")) {
                    // Average number of letters per word
                    double sum = 0.0;
                    for (String numLetters : values) {
                        sum += Integer.parseInt(numLetters);
                    }
                    output.add(new Record<String, String>(key, "" + (sum / values.size())));
                }
            }
        };
    }
}
