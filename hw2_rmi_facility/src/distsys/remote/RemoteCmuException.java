package distsys.remote;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/8/13
 */
public class RemoteCmuException extends IOException {

    /**
     * Default constructor
     */
    public RemoteCmuException() {
        super();
    }

    /**
     * Customer error message
     *
     * @param s Error message
     */
    public RemoteCmuException(String s) {
        super(s);
    }

}
