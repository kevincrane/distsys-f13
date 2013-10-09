package distsys.server;

import distsys.remote.RemoteKB;
import distsys.remote.RemoteKBException;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public interface MathSequences extends RemoteKB {
    public Long fibonacci(Integer n) throws RemoteKBException;

    public Integer nthPrime(Integer n) throws RemoteKBException;
}
