package distsys.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/10/13
 */

/**
 * Generic MapReduceJob class that encapsulates a Map Reduce job
 * User needs to set the inputFile and the outputFile for the Job
 * User also implements the getMapper and getReducer methods which provide those processes respectively
 *
 * @param <Kin>  Data type of the input key
 * @param <Vin>  Data type of the input value
 * @param <Kout> Data type of the output key
 * @param <Vout> Data type of the output value
 */

abstract public class MapReduceJob<Kin, Vin, Kout, Vout> {

    private String inputFile;
    private String outputFile;

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    abstract public Mapper<Kin, Vin, Kout, Vout> getMapper();

    abstract public Reducer<Kin, Vin, Kout, Vout> getReducer();
}