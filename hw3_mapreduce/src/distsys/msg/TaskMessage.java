package distsys.msg;

import distsys.mapreduce.Task;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/12/13
 */
public class TaskMessage extends Message {

    private Task task;

    TaskMessage(MessageType type, Task task) {
        super(type, task);

        this.task = task;
    }

    public Task getTask() {
        return task;
    }

}
