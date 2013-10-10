package distsys.registry;

import distsys.msg.*;
import distsys.remote.RemoteKB;
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
public class RmiRegistry extends Thread {

    private String rmiHostname;
    private int rmiPort;

    private Map<String, RemoteObjectReference> regRefs;
    public Map<String, RemoteKB> regObjs;
    private ServerSocket regServer;
    public final static int REG_PORT = 7341;

    public RmiRegistry(String hostname, int rmiPort) throws IOException {
        this.rmiHostname = hostname;
        this.rmiPort = rmiPort;

        regRefs = Collections.synchronizedMap(new HashMap<String, RemoteObjectReference>());
        regObjs = Collections.synchronizedMap(new HashMap<String, RemoteKB>());
        regServer = new ServerSocket(REG_PORT);
    }


    /**
     * Binds a new key name to a remote object reference; cannot overwrite existing keys
     *
     * @param keyName The unique key that the object can be referred as
     * @param remObj  A remote object to be bound
     * @throws distsys.remote.RemoteKBException
     *
     */
    public void bind(String keyName, RemoteKB remObj) throws RemoteKBException {
        if (regRefs.containsKey(keyName)) {
            // Throw exception if the key exists already
            throw new RemoteKBException("Key '" + keyName + "' already exists in the regRefs.");
        }

        // Add key name and remote object/reference to maps
        regObjs.put(keyName, remObj);
        regRefs.put(keyName, new RemoteObjectReference(rmiHostname, rmiPort, keyName, remObj.getClass().getName()));
        System.out.println("Registry: bound '" + remObj.getClass().getName() + "' object to key '" + keyName + "'.");
    }

    /**
     * Same as bind(), but allowed to overwrite existing keys
     *
     * @param keyName The key that the object can be referred as
     * @param remObj  A remote object to be bound
     */
    public void rebind(String keyName, RemoteKB remObj) {
        // Add key name and remote object/reference to maps
        regObjs.put(keyName, remObj);
        regRefs.put(keyName, new RemoteObjectReference(rmiHostname, rmiPort, keyName, remObj.getClass().getName()));

        System.out.println("Registry: bound '" + remObj.getClass().getName() + "' object to key '" + keyName + "'.");
    }

    /**
     * Return a reference to a bound object that is local on the server
     *
     * @param keyName Key name of bound object
     * @return Object bound locally in the registry
     */
    public RemoteKB localLookup(String keyName) {
        return regObjs.get(keyName);
    }


    /**
     * Returns a RemoteObjectReference corresponding to the key entered in the regRefs
     *
     * @param refKey Reference key
     * @return Corresponding RemoteObjectReference
     * @throws IllegalArgumentException
     */
    public RemoteObjectReference lookup(String refKey) throws IllegalArgumentException {
        if (regRefs.containsKey(refKey)) {
            return regRefs.get(refKey);
        } else {
            throw new IllegalArgumentException("Key " + refKey + " not found in RMI Registry");
        }
    }

    /**
     * Lists all available keys that have been entered into the regRefs
     *
     * @return Array of key strings
     */
    public String[] listKeys() {
        String[] keys = new String[regRefs.size()];
        int i = 0;
        for (String key : regRefs.keySet()) {
            keys[i] = key;
            i++;
        }

        return keys;
    }

    /**
     * Handle the operation requested in inMsg
     *
     * @param inMsg RMI Message requesting a particular operation
     * @return outMsg
     */
    private RmiMessage processMessage(RmiMessage inMsg) {
        RmiMessage outMsg;
        if (inMsg instanceof RmiRegLookupMessage) {
            // Perform a lookup operation
            System.out.println("Registry performing lookup action for key '" + ((RmiRegLookupMessage) inMsg).getRefKey() + "'.");
            try {
                RemoteObjectReference ref = lookup(((RmiRegLookupMessage) inMsg).getRefKey());
                outMsg = new RmiReturnMessage(ref);
            } catch (IllegalArgumentException e) {
                outMsg = new RmiExceptionMessage(e);
            }
        } else if (inMsg instanceof RmiRegListMessage) {
            // Perform a list() operation
            System.out.println("Registry performing list() action.");
            String[] refKeys = listKeys();
            outMsg = new RmiReturnMessage(refKeys);
        } else {
            outMsg = new RmiExceptionMessage(new IllegalArgumentException("Unknown regRefs action type."));
        }

        return outMsg;
    }


    /**
     * Registry runs as a thread that listens for incoming connections
     */
    @Override
    public void run() {
        System.out.println("Running RMI Registry on " + rmiHostname + ":" + rmiPort + ".");
        while (true) {
            try {
                // Accept a new connection from a client
                Socket newConnection = regServer.accept();
                CommHandler comm = new CommHandler(newConnection);

                // Receive an RMI message from the new client
                RmiMessage inMsg = comm.receiveMessage();

                // Perform a regRefs operation based on the type of message and send it back
                RmiMessage outMsg = processMessage(inMsg);
                comm.sendMessage(outMsg);
            } catch (IOException e) {
                System.err.println("Error: could not accept new regRefs socket connection (" + e.getMessage() + ").");
            }
        }
    }
}
