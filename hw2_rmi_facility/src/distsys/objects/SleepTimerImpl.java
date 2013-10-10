package distsys.objects;

import distsys.remote.RemoteKBException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 10/10/13
 */
public class SleepTimerImpl implements SleepTimer {
    @Override
    public void waitSeconds(Integer sec) throws RemoteKBException {
        try {
            Thread.sleep(sec * 1000);
            System.out.println("Finished waiting " + sec + " seconds.");
        } catch (InterruptedException e) {
            throw new RemoteKBException("SleepTimer thread interrupted.", e);
        }
    }
}
