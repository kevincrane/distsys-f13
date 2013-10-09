package distsys.remote;

import distsys.msg.*;
import distsys.registry.RemoteObjectReference;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public abstract class RemoteKBStub {
    RemoteObjectReference reference;

    public void setRemoteReference(RemoteObjectReference ref) {
        reference = ref;
    }

    public RemoteObjectReference getRemoteRef() {
        return reference;
    }

    /**
     * Remotely invoke a method on a server and return the result
     *
     * @param name Name of the remote method
     * @param args Remote method's arguments
     * @return RMI message result (method result or exception msg)
     * @throws RemoteKBException
     */
    protected Object invokeMethod(String name, Object[] args) throws RemoteKBException {
        // Create a new method invocation message
        RmiInvocationMessage outMsg = new RmiInvocationMessage(reference.getRefName(), name, args);
        RmiMessage returnMsg;
        Object returnValue;

        try {
            // Try to create an object for handling communication
            CommHandler comm = new CommHandler(reference.getHostname(), reference.getPort());

            // Send method invocation message
            comm.sendMessage(outMsg);

            // Wait for message receipt
            returnMsg = comm.receiveMessage();
        } catch (IOException e) {
            System.err.println("RemoteStub error: " + e.getMessage());
            throw new RemoteKBException("Stub: error with CommHandler. (" + e.getMessage() + ").", e);
        }

        // Check to see if method is valid result or exception
        if (returnMsg instanceof RmiReturnMessage) {
            returnValue = ((RmiReturnMessage) returnMsg).getReturnValue();
        } else if (returnMsg instanceof RmiExceptionMessage) {
            Exception cause = ((RmiExceptionMessage) returnMsg).getException();
            throw new RemoteKBException("Stub: exception from invoked method (" + cause.getMessage() + ");", cause);
        } else {
            throw new RemoteKBException("Stub: invalid RMI message type returned.");
        }

        return returnValue;
    }

}
