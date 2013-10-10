package distsys.msg;

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
        REG_LOOKUP,
        REG_LIST
    }

    private final MessageType type;
    private final Object payload;

    RmiMessage(MessageType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return type.name() + "{" + payload + "}";
    }

}
