package distsys.mapreduce;

import distsys.kdfs.DistFile;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/11/13
 */
public abstract class Task implements Serializable {
    protected int jobID;
    protected int slaveID;
    protected boolean running;
    protected boolean done;

    protected Task(int jobID, int slaveID) {
        this.jobID = jobID;
        this.slaveID = slaveID;
        running = false;
        done = false;
    }

    @Override
    public String toString() {
        return "Task: {jobId: " + jobID + ", slaveId: " + slaveID + ", running: " + running + ", done: " + done + "}";
    }
}