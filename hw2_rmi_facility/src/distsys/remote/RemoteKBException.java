package distsys.remote;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/8/13
 */
public class RemoteKBException extends IOException {

    /**
     * Default constructor
     */
    public RemoteKBException() {
        super();
    }

    /**
     * Customer error message
     *
     * @param s Error message
     */
    public RemoteKBException(String s) {
        super(s);
    }

}
