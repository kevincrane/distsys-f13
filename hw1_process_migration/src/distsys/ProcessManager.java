package distsys;

import distsys.server.MasterManager;
import distsys.server.SlaveManager;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 9/6/13
 */
public class ProcessManager {

    private static final int MASTER_PORT = 23456;

    /* TODO:
    public int start(Process process);
    public int remove(Process process);
    public int migrate(Process process);
    */


    /**
     * Main method starts here
     * @param args
     */
    public static void main(String[] args) {
        if(args.length == 2 && args[0].equals("-c")) {
            // Starting Slave ProcessManager on client machine
            String hostname = args[1];
            try {
                System.out.println("Running new slave node.");
                SlaveManager slave = new SlaveManager(hostname, MASTER_PORT);
                slave.listen();
                slave.close();
            } catch(IOException e) {
                System.err.println("Could not connect to socket at " + hostname + ":" + MASTER_PORT+ ".\n" +
                        e.getMessage());
            }
        } else if(args.length == 0) {
            // Starting Master ProcessManager on server machine
            try {
                System.out.println("Running new master node.");
                MasterManager master = new MasterManager(MASTER_PORT);
                master.start();

                // TODO: REMOVE THIS
                String[] countArgs = {"25"};
                master.addProcess("distsys.process.CountProcess", countArgs);
            } catch(IOException e) {
                System.err.println("Master could not listen at port " + MASTER_PORT + ".\n" + e.getMessage());
            }
        } else {
            System.out.println("usage: java ProcessManager [-c <master hostname>]");
        }
    }

}
