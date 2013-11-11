package distsys.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/10/13
 */
public class MapReduceJob {

    /**
     * Generic Mapper class for a Map Reduce job
     * User specifies the data type of the input/output keys and values,
     * and overrides the map(Kin, Vin) method
     *
     * @param <Kin>  Data type of the input key
     * @param <Vin>  Data type of the input value
     * @param <Kout> Data type of the output key
     * @param <Vout> Data type of the output value
     */
    class Mapper<Kin, Vin, Kout, Vout> implements Serializable {
        private List<Record<Kout, Vout>> output;

        public Mapper() {
            output = new ArrayList<Record<Kout, Vout>>();
        }

        /**
         * Map method called on every record read from the input file in KDFS. Takes a Record
         * from a DistFile (from DataNode), performs the map operation on it, and stores the
         * result in List output
         *
         * @param key   Key for mapper
         * @param value Value for reducer
         */
        public void map(Kin key, Vin value) {
            // Default map method just produces an identity mapper
            output.add(new Record<Kout, Vout>((Kout) key, (Vout) value));
        }

        /**
         * Returns the list of records created by running map() on an individual record
         *
         * @return Output list generated by map
         */
        public List<Record<Kout, Vout>> getMapOutput() {
            return output;
        }
    }

    /**
     * Generic Reducer class for a Map Reduce job
     * User specifies the data type of the input/output keys and values,
     * and overrides the map(Kin, Vin) method
     *
     * @param <Kin>  Data type of the input key
     * @param <Vin>  Data type of the input value
     * @param <Kout> Data type of the output key
     * @param <Vout> Data type of the output value
     */
    class Reducer<Kin, Vin, Kout, Vout> implements Serializable {
        private List<Record<Kout, Vout>> output;

        public Reducer() {
            output = new ArrayList<Record<Kout, Vout>>();
        }

        /**
         * Map method called on every record read from the input file in KDFS. Takes a Record
         * from a DistFile (from DataNode), performs the map operation on it, and stores the
         * result in List output
         *
         * @param key    Key for reducer
         * @param values List of all values corresponding to this key
         */
        public void reduce(Kin key, List<Vin> values) {
            // Default reduce method just produces an identity reducer
            for (Vin value : values) {
                output.add(new Record<Kout, Vout>((Kout) key, (Vout) value));
            }
        }

        /**
         * Returns the list of records created by running map() on an individual record
         *
         * @return Output list generated by reduce
         */
        public List<Record<Kout, Vout>> getReduceOutput() {
            return output;
        }
    }

}
