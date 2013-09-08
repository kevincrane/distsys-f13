package distsys;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 9/6/13
 * Time: 5:13 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ProcessManager {
    public abstract int start(Process process);
    public abstract int remove(Process process);
    public abstract int migrate(Process process);
}
