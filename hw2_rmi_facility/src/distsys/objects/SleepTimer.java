package distsys.objects;

import distsys.remote.RemoteKB;
import distsys.remote.RemoteKBException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/10/13
 */
public interface SleepTimer extends RemoteKB {
    public void waitSeconds(Integer sec) throws RemoteKBException;
}
