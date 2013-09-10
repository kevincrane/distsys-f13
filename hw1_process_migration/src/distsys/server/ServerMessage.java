package distsys.server;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/9/13
 */
public class ServerMessage implements Serializable {

    public static enum MessageType {
        HELLO,
        RUN,
        SUSPEND
    }

    public ServerMessage() {

    }

}
