package distsys.mapreduce;

import distsys.msg.CommHandler;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/13/13
 */

abstract public class TaskProcessor extends Thread {
    private Task task;
    private CommHandler masterComm;

    public TaskProcessor(Task task, CommHandler comm) {
        this.task = task;
        masterComm = comm;
    }

    protected CommHandler getMasterComm() {
        return masterComm;
    }

    //Process the Task
    @Override
    abstract public void run();
}
