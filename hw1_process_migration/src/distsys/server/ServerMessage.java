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
        SUSPEND,
        QUIT
    }

    private MessageType type;
    private Object payload;

    public ServerMessage(MessageType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return type.name() + "{" + payload + "}";
    }

    public MessageType getType() {
        return type;
    }

    public Object getPayload() {
        return payload;
    }

}
