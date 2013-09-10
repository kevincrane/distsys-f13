package distsys.server;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/9/13
 */
public class ServerMessage implements Serializable {

    public static enum MessageType {
        RUN,
        PING,
        SUSPEND
    }

    MessageType type;
    Object payload;

    public ServerMessage(MessageType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

}
