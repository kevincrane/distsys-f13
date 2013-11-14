package distsys.msg;

import distsys.mapreduce.Task;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 */

// Message for sending a Task to a slave to perform
public class TaskMessage extends Message {
    private Task task;

    public TaskMessage(Task task) {
        super(MessageType.TASK, task);
        this.task = task;
    }

    public Task getTask() {
        return task;
    }
}
