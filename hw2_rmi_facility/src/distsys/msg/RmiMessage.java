package distsys.msg;

import distsys.registry.RemoteObjectReference;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/5/13
 */
public abstract class RmiMessage implements Serializable {

    public static enum MessageType {
        METHOD,
        RETURN,
        EXCEPTION,
        OK
    }

    protected RemoteObjectReference reference;
    protected MessageType type;
    protected Object payload;

    public RmiMessage(MessageType type, Object payload) {
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
