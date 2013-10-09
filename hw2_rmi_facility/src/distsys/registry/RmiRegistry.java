package distsys.registry;

import distsys.msg.*;
import distsys.remote.RemoteKBException;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/8/13
 */
public class RmiRegistry implements Runnable {

    private Map<String, RemoteObjectReference> registry;
    private ServerSocket regServer;
    public final static int REG_PORT = 734;

    public RmiRegistry() throws IOException {
        registry = Collections.synchronizedMap(new HashMap<String, RemoteObjectReference>());
        regServer = new ServerSocket(REG_PORT);
    }


    /**
     * Binds a new key name to a remote object reference; cannot overwrite existing keys
     *
     * @param keyName   The unique key that the object can be referred as
     * @param remoteRef A remote object reference
     * @throws distsys.remote.RemoteKBException
     *
     */
    public void bind(String keyName, RemoteObjectReference remoteRef) throws RemoteKBException {
        if (registry.containsKey(keyName)) {
            // Throw exception if the key exists already
            throw new RemoteKBException("Key '" + keyName + "' already exists in the registry.");
        }
        registry.put(keyName, remoteRef);
    }

    /**
     * Same as bind(), but allowed to overwrite existing keys
     *
     * @param keyName   The key that the object can be referred as
     * @param remoteRef A remote object reference
     */
    public void rebind(String keyName, RemoteObjectReference remoteRef) {
        registry.put(keyName, remoteRef);
    }


    /**
     * Returns a RemoteObjectReference corresponding to the key entered in the registry
     *
     * @param refKey Reference key
     * @return Corresponding RemoteObjectReference
     * @throws IllegalArgumentException
     */
    public RemoteObjectReference lookup(String refKey) throws IllegalArgumentException {
        if (registry.containsKey(refKey)) {
            return registry.get(refKey);
        } else {
            throw new IllegalArgumentException("Key " + refKey + " not found in RMI Registry");
        }
    }

    /**
     * Lists all available keys that have been entered into the registry
     *
     * @return Array of key strings
     */
    public String[] listKeys() {
        String[] keys = new String[registry.size()];
        int i = 0;
        for (String key : registry.keySet()) {
            keys[i] = key;
            i++;
        }

        return keys;
    }


    /**
     * Registry runs as a thread that listens for incoming connections
     */
    @Override
    public void run() {
        while (true) {
            try {
                // Accept a new connection from a client
                Socket newConnection = regServer.accept();
                CommHandler comm = new CommHandler(newConnection);

                // Receive an RMI message from the new client
                RmiMessage inMsg = comm.receiveMessage();

                // Perform a registry operation based on the type of message
                RmiMessage outMsg;
                if (inMsg instanceof RmiRegLookupMessage) {
                    // Perform a lookup operation
                    try {
                        RemoteObjectReference ref = lookup(((RmiRegLookupMessage) inMsg).getRefKey());
                        outMsg = new RmiReturnMessage(ref);
                    } catch (IllegalArgumentException e) {
                        outMsg = new RmiExceptionMessage(e);
                    }
                } else if (inMsg instanceof RmiRegListMessage) {
                    // Perform a list() operation
                    String[] refKeys = listKeys();
                    outMsg = new RmiReturnMessage(refKeys);
                } else {
                    outMsg = new RmiExceptionMessage(new IllegalArgumentException("Unknown registry action type."));
                }

                // Send the response RMI message back
                comm.sendMessage(outMsg);
            } catch (IOException e) {
                System.err.println("Error: could not accept new registry socket connection (" + e.getMessage() + ").");
            }
        }
    }
}
