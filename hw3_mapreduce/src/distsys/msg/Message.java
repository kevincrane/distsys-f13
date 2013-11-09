package distsys.msg;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public abstract class Message implements Serializable {

    public static enum MessageType {
        ACK,
        BLOCKMAP,
        BLOCK_REQ,
        BLOCK_ADDR,
        BLOCK_CONTENT,
        BLOCK_POS,
        KILL
    }

    private final MessageType type;
    protected final Object payload;

    Message(MessageType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

    public MessageType getType() {
        return type;
    }

    @Override
    public String toString() {
        return type.name() + "{" + payload + "}";
    }

}
