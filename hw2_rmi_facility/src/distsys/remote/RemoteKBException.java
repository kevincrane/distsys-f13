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
        Throwable cause1 = cause;
    }

}
