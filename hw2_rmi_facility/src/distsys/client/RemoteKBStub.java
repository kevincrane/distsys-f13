package distsys.client;

import distsys.registry.RemoteObjectReference;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public abstract class RemoteKBStub {
    RemoteObjectReference reference;

    public void setRemoteReference(RemoteObjectReference ref) {
        reference = ref;
    }

    public RemoteObjectReference getRemoteRef() {
        return reference;
    }

}
