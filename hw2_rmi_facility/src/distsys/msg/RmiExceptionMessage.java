package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/7/13
 */
public class RmiExceptionMessage extends RmiMessage {

    private final Exception exception;

    /**
     * RMI Message, sending a thrown exception back to the calling program
     *
     * @param e The exception that was thrown
     */
    public RmiExceptionMessage(Exception e) {
        super(MessageType.EXCEPTION, e);
        this.exception = e;
    }

    public Exception getException() {
        return exception;
    }
}
