package distsys.msg;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/7/13
 */
public class RmiRegLookupMessage extends RmiMessage {

    private String refKey;

    /**
     * RMI Registry Lookup Message, requesting the remote object reference of a desired key
     *
     * @param refKey The returned value of the remote method
     */
    public RmiRegLookupMessage(String refKey) {
        super(MessageType.REG_LOOKUP, refKey);
        this.refKey = refKey;
    }


    public String getRefKey() {
        return refKey;
    }

    public void setRefKey(String refKey) {
        this.refKey = refKey;
        payload = refKey;
    }

}
