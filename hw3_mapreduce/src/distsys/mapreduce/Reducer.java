package distsys.mapreduce;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/12/13
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic Reducer class for a Map Reduce job
 * User specifies the data type of the input/output keys and values,
 * and overrides the reduce(Kin, List<Vin></Vin>) method
 *
 * @param <Kin>  Data type of the input key
 * @param <Vin>  Data type of the input value
 * @param <Kout> Data type of the output key
 * @param <Vout> Data type of the output value
 */
public abstract class Reducer<Kin, Vin, Kout, Vout> implements Serializable {
    protected List<Record<Kout, Vout>> output;

    public Reducer() {
        output = new ArrayList<Record<Kout, Vout>>();
    }

    /**
     * Reduce method called after shuffling and sorting of map results. Is fed a key
     * and all values associated with that key. Performs the reduce method on each value
     * and stores the resulting key/value pair in output list
     *
     * @param key    Key for reducer
     * @param values List of all values corresponding to this key
     */
    abstract public void reduce(Kin key, List<Vin> values);

    /**
     * Returns the list of records created by running map() on an individual record
     *
     * @return Output list generated by reduce
     */
    public List<Record<Kout, Vout>> getReduceOutput() {
        return output;
    }
}
