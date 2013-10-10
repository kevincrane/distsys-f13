package distsys;

import distsys.msg.*;
import distsys.objects.SleepTimer;
import distsys.registry.RemoteObjectReference;
import distsys.registry.RmiRegistry;
import distsys.remote.RemoteKBException;
import distsys.remote.RemoteStubProxy;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/10/13
 */
public class RmiClientSleep {
    // Test class for running remote math ops on a different machine
    private final static int REGISTRY_PORT = RmiRegistry.REG_PORT;
    private static String REGISTRY_HOSTNAME;


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
    public static void main(String[] args) {
        int sleepTime = 5;
        if (args.length < 1) {
            System.err.println("Usage: java distsys.RmiClientSleep server_hostname [sleep_time]");
            return;
        }
        // Set hostname from command line argument
        REGISTRY_HOSTNAME = args[0];

        // Get sleep time if available
        if (args.length >= 2) {
            try {
                sleepTime = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Usage: java distsys.RmiClientSleep server_hostname [sleep_time]");
                return;
            }
        }

        // Lookup SleepTimer reference from registry
        RemoteObjectReference ref;
        System.out.println("Requesting remote object reference 'sleep' from registry..");
        try {
            ref = lookupReference("sleep");
        } catch (IOException e) {
            System.err.println("Client error: could not communicate with registry (" + e.getMessage() + ").");
            return;
        }

        // Localise remote reference into a new stub, perform remote operations with it
        if (ref != null) {
            // Dynamically generate new stub class
            SleepTimer sleepTimer = (SleepTimer) RemoteStubProxy.newInstance(ref);

            try {
                System.out.println("Waiting for " + sleepTime + " seconds..");
                long startTime = System.currentTimeMillis();

                // Make remote RMI call through proxy method
                sleepTimer.waitSeconds(sleepTime);

                long endTime = System.currentTimeMillis();
                System.out.println("That was exactly " + (endTime - startTime) / 1000.0 + " seconds.");
            } catch (RemoteKBException e) {
                System.err.println("Something blew up remotely: " + e.getMessage());
            }
        }
    }

}
