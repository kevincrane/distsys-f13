package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/7/13
 */
public class RmiReturnMessage extends RmiMessage {

    private final Object returnValue;

    /**
     * RMI Message, sending the return value of the remote method that was called
     *
     * @param returnValue The returned value of the remote method
     */
    public RmiReturnMessage(Object returnValue) {
        super(MessageType.RETURN, returnValue);
        this.returnValue = returnValue;
    }


    public Object getReturnValue() {
        return returnValue;
    }

}
