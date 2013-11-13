package distsys.msg;

import distsys.mapreduce.Task;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 * Time: 3:01 AM
 * To change this template use File | Settings | File Templates.
 */

/**
 * Message for sending an update to master regarding the status of a task being processed
 * We need both running and done because a process might not be done, but it has stopped running because of some reasons
 * which might include failure
 *
 * TaskUpdate message also takes a Payload which in which the results of the final reduce can be sent back to master
 * for displaying to the user
 */
public class TaskUpdateMessage extends Message {
    private int jobId;
    private boolean running;
    private boolean done;

    public TaskUpdateMessage(int jobId, boolean running, boolean done) {
        super(MessageType.TASKUPDATE, jobId);
        this.jobId = jobId;
        this.running = running;
        this.done = done;
    }

    public TaskUpdateMessage(int jobId, boolean running, boolean done, Object payload) {
        super(MessageType.TASKUPDATE, payload);
        this.jobId = jobId;
        this.running = running;
        this.done = done;
    }

    public int getJobId() {
        return jobId;
    }
    public boolean isRunning() {
        return running;
    }

    public boolean isDone() {
        return done;
    }

    public Object getPayload() {
        return payload;
    }
}
