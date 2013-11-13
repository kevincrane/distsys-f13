package distsys.msg;

import distsys.mapreduce.Task;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 * Time: 2:59 AM
 * To change this template use File | Settings | File Templates.
 */

// Message for sending a Task to a slave to perform
public class TaskMessage extends Message {
    private Task task;

    public TaskMessage(Task task) {
        super(MessageType.TASK, task);
    }

    public Task getTask() {
        return task;
    }
}
