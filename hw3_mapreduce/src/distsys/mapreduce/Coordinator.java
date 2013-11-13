package distsys.mapreduce;

import distsys.Config;
import distsys.kdfs.NameNode;
import distsys.msg.CommHandler;
import distsys.msg.TaskMessage;
import distsys.msg.TaskUpdateMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/11/13
 */
public class Coordinator {

    private NameNode nameNode;
    // Map from JobId to Task
    private HashMap<Integer, Task> taskMap = new HashMap<Integer, Task>();

    public Coordinator(NameNode nameNode) {
        this.nameNode = nameNode;
    }

    /**
     * Main entry point for scheduling tasks
     * Coordinator - Takes care of scheduling job on slaves, handling failure in case a task or a slave node failed
     * as well as scheudling reducers on mapper completions
     *
     * @param tasks Takes in a list of tasks which include MapperTasks as well as ReducerTasks
     */
    public void scheduleTasks(List<Task> tasks) {
        for (Task task : tasks) {
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
                    slaveComm.receiveMessage();
                } catch (IOException e) {
                    // TODO handle case where slave is down, retry on different slave, maxTries of 3, if still doesn't work throw job return failure
                    e.printStackTrace();
                }
            }
            // add every task to queue to keep track of them and retry them upon failure
            taskMap.put(task.getJobID(), task);
        }
    }

    /**
     * Master Node receives the task update message that was sent by a slave and sends that to the CoOrdinator
     *
     * @param msg TaskUpdateMessage that was received from a slave indicating completion status of a particular task
     */
    public void processTaskUpdateMessage(TaskUpdateMessage msg) {
        System.out.println("Received message from slave that taskId: " + msg.getJobId() + "is done: " + msg.isDone());
        Task targetTask = taskMap.get(msg.getJobId());
        // if no task exists with that job id then we ignore the message
        if (targetTask == null)
            return;

        //If it is a Map task, update the particular map task as well as reduce tasks that are dependant on that map
        if (targetTask instanceof MapperTask) {
            // changes status job whose status changed
            targetTask.running = msg.isRunning();
            targetTask.done = msg.isDone();

            // changes waiting status of reduce jobs that have the map job as it's dependant
            for (Task task : taskMap.values()) {
                if (task instanceof ReducerTask) {
                    ReducerTask reducerTask = (ReducerTask) task;
                    reducerTask.setMapperJobStatus(msg.getJobId(), msg.isDone());
                    if (reducerTask.allMappersAreReady()) {
                        //schedule reducer task on a slave
                        System.out.println("ALL MAPS DETECTED AS READY, SCHEDULING REDUCE!!! YESSS!!!! :D for task with jobID: " + reducerTask.getJobID());
                        scheduleReducer(reducerTask);
                    }
                }
            }
        } else if (targetTask instanceof ReducerTask) {
            if (msg.isDone()) {
                // ReducerTask is done, remove reducer as well as all dependant map tasks from Co-ordinator's Queue
                // Remove dependant maps
                for (int jobID : ((ReducerTask) targetTask).getDependentMapperJobIds()) {
                    taskMap.remove(jobID);
                }
                // Remove reduce job
                taskMap.remove(msg.getJobId());

                displayReduceResultsToUser(msg.getPayload());
            } else if (!msg.isRunning()) {
                // not done, but not running, FAILED on that slave
                int maxRetries = Config.MAX_JOB_RETRIES;
                while (maxRetries > 0) {
                    maxRetries--;
                    break;
                    //TODO HANDLE FAILURE of reduce step from slave, send to another slave with a max tries from the config
                }
            }
        }
    }

    // TODO not final way for scheduling, only for initial testing purposes
    private int getChillestSlaveId() {
        int minSlave = -1;
        int minJobs = Integer.MAX_VALUE;
        for (int slaveId : nameNode.getSlaveIds()) {
            int numJobs = getRunningTasks(slaveId).size();
            if (numJobs < minJobs) {
                minJobs = numJobs;
                minSlave = slaveId;
            }
        }
        System.out.println("Current Chillest slave is " + minSlave + ", so sending task to it");
        return minSlave;
    }

    /**
     * Gives a list of running tasks for a particular Slave
     *
     * @param slaveId Id of slave for which we are requesting running tasks
     * @return List of running tasks for a particular slave node
     */
    protected List<Task> getRunningTasks(int slaveId) {
        List<Task> runningTasks = new ArrayList<Task>();
        for (Task task : taskMap.values()) {
            if (task.slaveID == slaveId && task.running) {
                runningTasks.add(task);
            }
        }
        return runningTasks;
    }


    //TODO: make this match up with whatever Bala has
    public void scheduleReducer(ReducerTask reducerTask) {
        //todo for each living slave, if it has space in taskMap, open CommHandler, send new TaskMessage(ReducerTask)
    }

    public void displayReduceResultsToUser(Object finalReducerResults) {
        System.out.println("REDUCE FINISHED: \n Results: \n " + finalReducerResults);
    }
}


//TODO BALA, start here; split Job into Tasks by how many blocks the file has (namespace.get(newJob.getInputFile()).size())
//  e.g. 9 blocks -> 9 tasks (Task - Mapper, filename, start index, end index)
//  distribute the tasks (find the next slave, send a MapperMessage to slave)
//      Which slaves have which blocks, which slaves are least full, queue of tasks that can't be scheduled yet
//  On the Slave node
//      when it receives MapperMessage, execute map, write the output to a file (key \t value\n)
//      Somehow, send output file to partitioner, send Master a message saying you're done
