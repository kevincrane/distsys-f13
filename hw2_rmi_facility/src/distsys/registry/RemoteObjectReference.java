package distsys.registry;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/7/13
 */
public class RemoteObjectReference {

    private String hostname;      // IP address & port of registry
    private int port;
    private String refName;             // Name of the object reference in the registry
    private String className;           // Class of the object that is referenced

    /**
     * Reference to a remote object held in an RMI registry
     *
     * @param hostname  IP address of registry
     * @param port      Port of registry
     * @param refName   Name of the object reference in the registry
     * @param className Class of the object that is referenced
     */
    public RemoteObjectReference(String hostname, int port, String refName, String className) {
        this.hostname = hostname;
        this.port = port;
        this.refName = refName;
        this.className = className;
    }

    public String getHostname() {
        return hostname;
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

    /**
     * Creates a new stub object of the type contained within RemoteObjectReference
     *
     * @return new stub object
     */
    public Object localise() {
        //TODO: add method to create local stub of the remote object
        return "test test test!";
    }
}
