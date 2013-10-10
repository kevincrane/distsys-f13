package distsys;

import distsys.msg.*;
import distsys.objects.MathSequences;
import distsys.objects.MathSequencesImpl;
import distsys.objects.SleepTimer;
import distsys.objects.SleepTimerImpl;
import distsys.registry.RmiRegistry;
import distsys.remote.RemoteKBException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public class RmiServer {
    private final static int RMI_PORT = 7641;
    private static RmiRegistry registry;


    /**
     * Performs a desired action based on RMI message received
     *
     * @param sock Socket to remote client
     * @throws IOException
     */
    private static void handleConnection(Socket sock) throws IOException {
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
     * @param invokeMsg RMI invocation message
     */
    private static Object invokeMethod(RmiInvocationMessage invokeMsg) {
        // Grab reference to the desired object from the local table
        Object localObj = registry.localLookup(invokeMsg.getRefName());
        Object[] methodArgs = invokeMsg.getMethodArgs();
        Object returnValue = null;

        try {
            // Try to define the actual method we're invoking
            if (methodArgs == null) {
                // No arguments to call
                Method method = localObj.getClass().getMethod(invokeMsg.getMethodName());

                // Invoke the method!
                returnValue = method.invoke(localObj);

            } else {
                // Arguments were passed with the method name
                Class<?>[] argTypes = new Class[methodArgs.length];
                for (int i = 0; i < methodArgs.length; i++) {
                    argTypes[i] = methodArgs[i].getClass();
                }
                Method method = localObj.getClass().getMethod(invokeMsg.getMethodName(), argTypes);

                // Invoke the method!
                System.out.println("Invoking method '" + invokeMsg.getMethodName() + "' on object '" +
                        invokeMsg.getRefName() + "'.");
                returnValue = method.invoke(localObj, methodArgs);
            }

        } catch (NoSuchMethodException e) {
            System.err.println("Boo, no such method '" + invokeMsg.getMethodName() + "'!");
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
        String RMI_HOSTNAME = InetAddress.getLocalHost().getCanonicalHostName();

        // Instantiate and run the RMI Registry for this server
        registry = new RmiRegistry(RMI_HOSTNAME, RMI_PORT);
        registry.start();

        // Create two remote objects
        MathSequences maths = new MathSequencesImpl();
        registry.rebind("maths", maths);
        SleepTimer sleep = new SleepTimerImpl();
        registry.rebind("sleep", sleep);

        // Run loop and listen for incoming connections
        while (true) {
            final Socket newConnection = server.accept();

            // Handle this request in a new thread
            new Thread(new Runnable() {
                public void run() {
                    try {
                        handleConnection(newConnection);
                    } catch (IOException e) {
                        System.err.println("Error: Did not handle request from incoming msg properly (" + e.getMessage() + ").");
                    }
                }
            }).start();
        }
    }

}
