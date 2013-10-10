package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 10/7/13
 */
public class RmiInvocationMessage extends RmiMessage {

    private final String methodName;
    private final Object[] methodArgs;
    private final String refName;

    /**
     * RMI Message, sending the name of the remote method to be called and its arguments
     *
     * @param refName    The registry key of the object that should be invoked
     * @param methodName The name of the method to be called
     * @param methodArgs The remote method's arguments
     */
    public RmiInvocationMessage(String refName, String methodName, Object[] methodArgs) {
        super(MessageType.METHOD, new Object[]{refName, methodName, methodArgs});
        this.refName = refName;
        this.methodName = methodName;
        this.methodArgs = methodArgs;
    }


    public String getMethodName() {
        return methodName;
    }

    public Object[] getMethodArgs() {
        return methodArgs;
    }

    public String getRefName() {
        return refName;
    }
}
