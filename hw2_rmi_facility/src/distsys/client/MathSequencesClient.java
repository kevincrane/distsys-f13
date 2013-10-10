package distsys.client;

import distsys.registry.RemoteObjectReference;
import distsys.remote.RemoteKBException;
import distsys.server.RmiServer;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public class MathSequencesClient {
    // Test class for running remote math ops on a different machine
    final static String serverHostname = "localhost";
    final static int serverPort = RmiServer.RMI_PORT;


    // Main Method
    public static void main(String[] args) {
        //TODO: connect to a registry
        //TODO: check to see which objects are registered
        //TODO: lookup math object


        RemoteObjectReference ref = new RemoteObjectReference(serverHostname, serverPort, "maths", "distsys.client.MathSequences");
        MathSequences_stub mathSequences = (MathSequences_stub) ref.localise();

        try {
            Long fib = mathSequences.fibonacci(100);
            System.out.println("The 100th Fibonacci number is " + fib);
            Integer prime = mathSequences.nthPrime(1000000);
            System.out.println("The 1,000,000th prime number is " + prime);
        } catch (RemoteKBException e) {
            System.err.println("Something blew up remotely: " + e.getMessage());
        }
    }
}
