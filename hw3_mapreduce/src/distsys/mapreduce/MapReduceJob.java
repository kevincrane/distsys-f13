package distsys.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/10/13
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