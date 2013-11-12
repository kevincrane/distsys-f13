package distsys.mapreduce;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/12/13
 * Time: 3:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class IdentityMapReduceJob<Kin, Vin, Kout, Vout> extends MapReduceJob {

    public Mapper<Kin, Vin, Kout, Vout> getMapper() {
        return new Mapper<Kin, Vin, Kout, Vout>() {
            @Override
            public void map(Kin key, Vin value) {
                // Default map method just produces an identity mapper
                output.add(new Record<Kout, Vout>((Kout) key, (Vout) value));
            }
        };
    }

    public Reducer<Kin, Vin, Kout, Vout> getReducer() {
        return new Reducer<Kin, Vin, Kout, Vout>() {
            @Override
            public void reduce(Kin key, List<Vin> values) {
                // Default reduce method just produces an identity reducer
                for (Vin value : values) {
                    output.add(new Record<Kout, Vout>((Kout) key, (Vout) value));
                }
            }
        };
    }
}
