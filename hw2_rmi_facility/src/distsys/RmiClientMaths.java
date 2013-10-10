package distsys;

import distsys.msg.*;
import distsys.objects.MathSequences;
import distsys.registry.RemoteObjectReference;
import distsys.registry.RmiRegistry;
import distsys.remote.RemoteKBException;
import distsys.remote.RemoteStubProxy;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public class RmiClientMaths {
    // Test class for running remote math ops on a different machine
    private final static int REGISTRY_PORT = RmiRegistry.REG_PORT;
    private static String REGISTRY_HOSTNAME;


    /**
     * Test method to demonstrate remotely calling list() on the registry
     */
    private static void printRegistryKeys() throws IOException {
        // Send RMI message directly to registry
        CommHandler registryComm = new CommHandler(REGISTRY_HOSTNAME, REGISTRY_PORT);
        registryComm.sendMessage(new RmiRegListMessage());
        RmiMessage inMsg = registryComm.receiveMessage();

        if (inMsg instanceof RmiReturnMessage) {
            String[] keys = (String[]) ((RmiReturnMessage) inMsg).getReturnValue();
            if (keys.length <= 0) {
                // No keys returned in registry
                System.out.println("No registry keys.");
            } else {
                // At least one key found, print them all out
                System.out.print(keys[0]);
                for (int i = 1; i < keys.length; i++) {
                    System.out.print(", " + keys[i]);
                }
                System.out.println();
            }
        } else {
            System.out.println("Error: invalid RMI message type returned.");
        }
    }


    /**
     * Test method to lookup a reference from the registry
     */
    private static RemoteObjectReference lookupReference(String refName) throws IOException {
        // Send registry message looking up key refName
        RemoteObjectReference ref;
        CommHandler registryComm = new CommHandler(REGISTRY_HOSTNAME, REGISTRY_PORT);
        registryComm.sendMessage(new RmiRegLookupMessage(refName));

        // Receive a message back and act on it
        RmiMessage inMsg = registryComm.receiveMessage();
        if (inMsg instanceof RmiReturnMessage) {
            // Read the reference from the message
            ref = (RemoteObjectReference) ((RmiReturnMessage) inMsg).getReturnValue();
            System.out.println("  Received remote reference of class type '" + ref.getClassName() + "'.");
        } else if (inMsg instanceof RmiExceptionMessage) {
            // Registry threw an exception
            System.out.println("Remote Exception: " + ((RmiExceptionMessage) inMsg).getException().getMessage());
            return null;
        } else {
            // Some other RMI message came back?
            System.err.println("Client Error: registry did not send a proper RMI return message.");
            return null;
        }

        return ref;
    }


    // Main Method
    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (args.length != 1) {
            System.out.println("Usage: java distsys.RmiClientMaths server_hostname");
            return;
        }
        REGISTRY_HOSTNAME = args[0];

        // First demonstration - list all keys on registry
        System.out.print("1. The following key names are currently registered:\n  ");
        try {
            printRegistryKeys();
        } catch (IOException e) {
            System.err.println("Client error: could not communicate with registry (" + e.getMessage() + ").");
            return;
        }

        // Second demo - lookup remote object reference
        RemoteObjectReference ref;
        System.out.println("2. Requesting remote object reference 'maths' from registry..");
        try {
            ref = lookupReference("maths");
        } catch (IOException e) {
            System.err.println("Client error: could not communicate with registry (" + e.getMessage() + ").");
            return;
        }

        // Localise remote reference into a new stub, perform remote operations with it
        if (ref != null) {
            // Dynamically generate new stub class
            MathSequences mathSequences = (MathSequences) RemoteStubProxy.newInstance(ref);

            try {
                // Make two different remote proxy calls
                Long fib = mathSequences.fibonacci(100);
                System.out.println("\nThe 100th Fibonacci number is " + fib);
                Integer prime = mathSequences.nthPrime(2000000);
                System.out.println("The 2,000,000th prime number is " + prime);
            } catch (RemoteKBException e) {
                System.err.println("Something blew up remotely: " + e.getMessage());
            }
        }
    }
}
