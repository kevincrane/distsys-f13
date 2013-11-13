package distsys.mapreduce;

import distsys.Config;
import distsys.kdfs.NameNode;
import distsys.msg.CommHandler;
import distsys.msg.TaskMessage;
import distsys.msg.TaskUpdateMessage;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/11/13
 */
public class Coordinator {

    private NameNode nameNode;
    private Queue<Task> taskQueue = new LinkedList<Task>();

    public Coordinator(NameNode nameNode) {
        this.nameNode = nameNode;
    }

    public void scheduleTasks(List<Task> tasks) {
        for (Task task: tasks) {
            System.out.println("Task received for scheduling: \n" + task);
            // schedule mappers immediately, reducers scheduled only when the mappers are ready
            if (task instanceof MapperTask) {
                // assign correct slaveIds for each task
                task.slaveID = getChillestSlaveId();
                // send tasks to slaves to begin processing
                try {
                    CommHandler slaveComm = new CommHandler(Config.SLAVE_NODES[task.slaveID][0], Config.SLAVE_NODES[task.slaveID][1]);
                    slaveComm.sendMessage(new TaskMessage(task));
                    // set running in each task processed to one
                    task.running = true;
                } catch (IOException e) {
                    // TODO handle case where slave is down, retry on different slave, maxTries of 3, if still doesn't work throw job return failure
                    e.printStackTrace();
                }
            }
        }
        // add every task to queue to keep track of them and retry them upon failure
        taskQueue.addAll(tasks);
    }

    /**
     * Master Node receives the task update message that was sent by a slave and sends that to the CoOrdinator
     * @param msg TaskUpdateMessage that was received from a slave indicating completion status of a particular task
     */
    public void processTaskUpdateMessage(TaskUpdateMessage msg) {
        for (Task task: taskQueue) {
            // changes status job whose status changed
            if (task.jobID == msg.getJobId())
            {
                task.running = msg.isRunning();
                task.done = msg.isDone();
                //TODO if not running and is not done, something failed in slave, need to handle appropriately
            // also changes ready status of reduce jobs
            } else if(task instanceof ReducerTask) {
                ReducerTask reducerTask = (ReducerTask) task;
                reducerTask.setMapperJobStatus(msg.getJobId(), msg.isDone());
                if (reducerTask.allMappersAreReady()) {
                    //schedule reducer task on a slave
                    System.out.println("ALL MAPS DETECTED AS READY, SCHEDULING REDUCE!!! YESSS!!!! :D for task with jobID: " + reducerTask.jobID);
                }
            }
        }



        //TODO reducer jobs need to be scheduled
    }

    // not final way for scheduling, fine for initial testing purposes
    private int getChillestSlaveId() {
        int minSlave = -1;
        int minJobs = Integer.MAX_VALUE;
        for (int slaveId: nameNode.getSlaveIds()) {
            int numJobs = getRunningTasks(slaveId).size();
            if (numJobs < minJobs) {
                minJobs = numJobs;
                minSlave = slaveId;
            }
        }
        return minSlave;
    }

    protected List<Task> getRunningTasks(int slaveId) {
        List<Task> runningTasks = new ArrayList<Task>();
        for (Task task: taskQueue) {
            if (task.running && task.slaveID == slaveId) {
                runningTasks.add(task);
            }
        }
        return runningTasks;
    }

    //TODO: make this match up with whatever Bala has
    public void beginReducers(int jobId, MapReduceJob job) {
        //todo for each living slave, if it has space in taskQueue, open CommHandler, send new TaskMessage(ReducerTask)
    }
}


//TODO BALA, start here; split Job into Tasks by how many blocks the file has (namespace.get(newJob.getInputFile()).size())
//  e.g. 9 blocks -> 9 tasks (Task - Mapper, filename, start index, end index)
//  distribute the tasks (find the next slave, send a MapperMessage to slave)
//      Which slaves have which blocks, which slaves are least full, queue of tasks that can't be scheduled yet
//  On the Slave node
//      when it receives MapperMessage, execute map, write the output to a file (key \t value\n)
//      Somehow, send output file to partitioner, send Master a message saying you're done
