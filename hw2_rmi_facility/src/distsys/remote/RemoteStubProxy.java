package distsys.remote;

import distsys.registry.RemoteObjectReference;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/10/13
 */
public class RemoteStubProxy implements InvocationHandler {

    private final RemoteKBStub newStub = new RemoteKBStub();

    /**
     * Generates a new proxy handler for generating stub proxy methods
     *
     * @param ref Reference to a remote object in the registry
     * @return A new instance of the interface passed in by REF
     */
    public static Object newInstance(RemoteObjectReference ref) {
        Object tempClass = ref.localise();

        return Proxy.newProxyInstance(tempClass.getClass().getClassLoader(),
                tempClass.getClass().getInterfaces(), new RemoteStubProxy(ref));
    }

    private RemoteStubProxy(RemoteObjectReference ref) {
        newStub.setRemoteReference(ref);
    }

    /**
     * Automatically called as contents to any method in the interface described by REF
     *
     * @param proxy  Proxy object (not needed)
     * @param method Method that is being called
     * @param args   Arguments to this method
     * @return The return value of the method (found using invoke() and remote connections)
     * @throws Throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object returnValue;
        returnValue = newStub.invokeMethod(method.getName(), args);

        if (!returnValue.getClass().equals(method.getReturnType())) {
            throw new RemoteKBException("Invalid method return type.");
        }

        return returnValue;
    }
}
