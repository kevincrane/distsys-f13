package distsys;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class MapReduceManager {

    /**
     * Main method
     */
    public static void main(String[] args) throws IOException {
        System.out.println("Welcome to CMU MapReduce!\n");
        System.out.println("Starting MasterNode thread..");

        // Start MasterNode thread
        MasterNode masterNode = new MasterNode();
        masterNode.start();
    }

}
