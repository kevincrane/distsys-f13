package distsys.process;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 9/6/13
 */
public interface MigratableProcess extends Runnable, Serializable {

    /**
     * Used to perform any necessary cleanup or safety procedures before
     * pausing the process for migration
     */
    void suspend();

}
