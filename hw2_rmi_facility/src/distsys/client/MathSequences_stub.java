package distsys.client;

import distsys.remote.RemoteKBException;
import distsys.remote.RemoteKBStub;
import distsys.server.MathSequences;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public class MathSequences_stub extends RemoteKBStub implements MathSequences {

    //TODO: how auto-generate stubs?


    @Override
    public Long fibonacci(Integer n) throws RemoteKBException {
        Object returnValue;
        returnValue = invokeMethod("fibonacci", new Object[]{n});

        if (!(returnValue instanceof Long)) {
            throw new RemoteKBException("Invalid method return type.");
        }
        return (Long) returnValue;
    }

    @Override
    public Integer nthPrime(Integer n) throws RemoteKBException {
        Object returnValue;
        returnValue = invokeMethod("nthPrime", new Object[]{n});

        if (!(returnValue instanceof Integer)) {
            throw new RemoteKBException("Invalid method return type.");
        }
        return (Integer) returnValue;
    }
}
