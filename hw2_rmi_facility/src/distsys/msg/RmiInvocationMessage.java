package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 10/7/13
 */
public class RmiInvocationMessage extends RmiMessage {

    private String methodName;
    private Object[] methodArgs;
    private String refName;

    /**
     * RMI Message, sending the name of the remote method to be called and its arguments
     *
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

    public void setMethodName(String methodName) {
        this.methodName = methodName;
        ((Object[]) payload)[1] = methodName;    //TODO: typecheck this
    }

    public Object[] getMethodArgs() {
        return methodArgs;
    }

    public void setMethodArgs(Object[] methodArgs) {
        this.methodArgs = methodArgs;
        ((Object[]) payload)[2] = methodArgs;    //TODO: typecheck this
    }

    public String getRefName() {
        return refName;
    }

    public void setRefName(String refName) {
        this.refName = refName;
        ((Object[]) payload)[0] = refName;
    }

}
