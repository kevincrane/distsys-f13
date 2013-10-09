package distsys.server;

import distsys.msg.*;
import distsys.remote.RemoteKB;
import distsys.remote.RemoteKBException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public class RmiServer {
    final public static int RMI_PORT = 11223;

    public static Map<String, RemoteKB> remoteObjects = Collections.synchronizedMap(new HashMap<String, RemoteKB>());


    public static void handleConnection(Socket sock) throws IOException {
        // Create CommHandler
        CommHandler comm = new CommHandler(sock);
        RmiMessage inMsg = comm.receiveMessage();
        RmiMessage outMsg;

        // Invoke method if appropriate
        if (inMsg instanceof RmiInvocationMessage) {
            RmiInvocationMessage invoke = (RmiInvocationMessage) inMsg;
            try {
                Object returned = invokeMethod(invoke);
                outMsg = new RmiReturnMessage(returned);
            } catch (Exception e) {
                outMsg = new RmiExceptionMessage(e);
            }
        } else {
            outMsg = new RmiExceptionMessage(new RemoteKBException("No valid action found."));
        }

        // Send the new message back
        comm.sendMessage(outMsg);
    }

    /**
     * Invoke the method described within an RMI invocation message
     *
     * @param invoke RMI invocation message
     */
    private static Object invokeMethod(RmiInvocationMessage invoke) throws Exception {
        // Grab reference to the desired object from the local table
        Object localObj = remoteObjects.get(invoke.getRefName());
        Object[] methodArgs = invoke.getMethodArgs();
        Method meth;
        Object returnValue = null;

        try {
            // Try to define the actual method we're invoking
            if (methodArgs == null) {
                // No arguments to call
                meth = localObj.getClass().getMethod(invoke.getMethodName());
                // Invoke the method!
                returnValue = meth.invoke(localObj);

            } else {
                // Arguments were passed with the method name
                Class<?>[] classTypes = new Class[methodArgs.length];
                for (int i = 0; i < methodArgs.length; i++) {
                    classTypes[i] = methodArgs[i].getClass();
                }
                meth = localObj.getClass().getMethod(invoke.getMethodName(), classTypes);

                // Invoke the method!
                returnValue = meth.invoke(localObj, methodArgs);
            }
        } catch (NoSuchMethodException e) {
            System.err.println("Boo, no such method '" + invoke.getMethodName() + "'!");
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            System.err.println("Boo, invocation error!");
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            System.err.println("Boo, illegal access error!");
            e.printStackTrace();
        }

        // Return the value made from the invoked method
        return returnValue;
    }


    // Main method
    public static void main(String[] args) throws IOException {
        // Initialize server socket
        ServerSocket server = new ServerSocket(RMI_PORT);

        //TODO: add registry stuff

        // Create remote object
        MathSequences maths = new MathSequencesImpl();
        remoteObjects.put("maths", maths);
        //TODO add maths to real registry

        // Run loop and listen for incoming connections
        while (true) {
            //TODO: create new threads on accepts, break sometime?
            Socket newConnection = server.accept();
            System.out.println("Got a new connection from " + newConnection.getInetAddress().getCanonicalHostName() + "!");
            handleConnection(newConnection);
        }
    }

}
