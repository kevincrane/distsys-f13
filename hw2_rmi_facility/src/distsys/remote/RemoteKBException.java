package distsys.remote;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/8/13
 * <p/>
 * Referenced from:
 * http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/6-b14/java/rmi/RemoteException.java#RemoteException
 */
public class RemoteKBException extends Exception {

    private Throwable cause = null;

    /**
     * Custom error message
     *
     * @param s Error message
     */
    public RemoteKBException(String s) {
        super(s);
    }

    /**
     * Custom error message and root cause
     *
     * @param s     Error message
     * @param cause Root cause
     */
    public RemoteKBException(String s, Throwable cause) {
        super(s);
        this.cause = cause;
    }

    /**
     * Returns a custom message if there is a nested exception that caused it
     *
     * @return New exception message string
     */
    @Override
    public String getMessage() {
        if (cause == null) {
            return super.getMessage();
        } else {
            return super.getMessage() + "; nested exception is: \n\t" + cause.toString();
        }
    }

}
