package distsys.mapreduce;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/11/13
 */
public abstract class Task implements Serializable {
    private int jobID;
    protected int slaveID;
    protected boolean running;
    protected boolean done;

    protected Task(int jobID, int slaveID) {
        this.jobID = jobID;
        this.slaveID = slaveID;
        running = false;
        done = false;
    }

    public int getJobID() {
        return jobID;
    }

    public int getSlaveID() {
        return slaveID;
    }

    @Override
    public String toString() {
        return "Task: {jobId: " + jobID + ", slaveId: " + slaveID + ", running: " + running + ", done: " + done + "}";
    }
}