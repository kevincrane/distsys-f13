package distsys;

import java.net.InetAddress;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/7/13
 */
public class RemoteObjectReference {

    private InetAddress ipAddress;      // IP address & port of registry
    private int port;
    private String refName;             // Name of the object reference in the registry
    private String className;           // Class of the object that is referenced

    /**
     * Reference to a remote object held in an RMI registry
     * @param ipAddress    IP address of registry
     * @param port         Port of registry
     * @param refName      Name of the object reference in the registry
     * @param className    Class of the object that is referenced
     */
    public RemoteObjectReference(InetAddress ipAddress, int port, String refName, String className) {
        this.ipAddress = ipAddress;
        this.port = port;
        this.refName = refName;
        this.className = className;
    }

    public InetAddress getIpAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    public String getRefName() {
        return refName;
    }

    public String getClassName() {
        return className;
    }

    //TODO: add method to create local stub of the remote object
}
