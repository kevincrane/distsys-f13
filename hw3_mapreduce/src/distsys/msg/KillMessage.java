package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/6/13
 */
public class KillMessage extends Message {

    // Simple Message for acknowledgements
    public KillMessage() {
        super(MessageType.KILL, null);
    }

}
