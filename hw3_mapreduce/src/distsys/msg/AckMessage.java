package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class AckMessage extends Message {

    // Simple Message for acknowledgements
    public AckMessage() {
        super(MessageType.ACK, null);
    }

}
