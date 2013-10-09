package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/7/13
 */
public class RmiRegListMessage extends RmiMessage {

    /**
     * RMI RegList Message, requesting a list of available references in the registry
     */
    public RmiRegListMessage() {
        super(MessageType.REG_LIST, null);
    }

}
