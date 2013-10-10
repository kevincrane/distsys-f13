package distsys.registry;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/7/13
 */
public class RemoteObjectReference implements Serializable {

    private final String hostname;      // IP address & port of registry
    private final int port;
    private final String refName;             // Name of the object reference in the registry
    private final String className;           // Class of the object that is referenced

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
        try {
            // Create new class from the name of the stub class
            return Class.forName(className).newInstance();

        } catch (ClassNotFoundException e) {
            System.err.println("ROR Error: class not found (" + e.getMessage() + ").");
        } catch (InstantiationException e) {
            System.err.println("ROR Error: class could not be instantiated (" + e.getMessage() + ").");
        } catch (IllegalAccessException e) {
            System.err.println("ROR Error: class could not be accessed (" + e.getMessage() + ").");
        }
        return null;
    }
}
