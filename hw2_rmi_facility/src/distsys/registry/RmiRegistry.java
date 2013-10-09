package distsys.registry;

import distsys.remote.RemoteCmuException;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/8/13
 */
public class RmiRegistry {

    private Map<String, RemoteObjectReference> registry;
    private InetAddress ipAddress;
    private int port;

    public RmiRegistry() {
        registry = Collections.synchronizedMap(new HashMap<String, RemoteObjectReference>());
    }


    /**
     * Binds a new key name to a remote object reference; cannot overwrite existing keys
     *
     * @param keyName   The unique key that the object can be referred as
     * @param remoteRef A remote object reference
     * @throws RemoteCmuException
     */
    public void bind(String keyName, RemoteObjectReference remoteRef) throws RemoteCmuException {
        if (registry.containsKey(keyName)) {
            // Throw exception if the key exists already
            throw new RemoteCmuException("Key '" + keyName + "' already exists in the registry.");
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


}
