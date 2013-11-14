package distsys.mapreduce;

import distsys.Config;
import distsys.kdfs.NameNode;
import distsys.msg.CommHandler;
import distsys.msg.TaskMessage;
import distsys.msg.TaskUpdateMessage;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/11/13
 */
public class Coordinator {

    private NameNode nameNode;
    // Map from JobId to Task
    private Map<Integer, Task> taskMap;

    public Coordinator(NameNode nameNode) {
        this.nameNode = nameNode;
        this.taskMap = Collections.synchronizedMap(new HashMap<Integer, Task>());
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
            System.out.println("Task received for scheduling:\n  " + task);
            taskMap.put(task.getJobID(), task);

            // schedule mappers immediately, reducers scheduled only when the mappers are ready
            if (task instanceof MapperTask) {
                // assign correct slaveIds for each task
                task.slaveID = getChillestSlaveId((MapperTask) task);
                // send tasks to slaves to begin processing
                try {
                    CommHandler slaveComm = new CommHandler(Config.SLAVE_NODES[task.slaveID][0],
                            Config.SLAVE_NODES[task.slaveID][1]);
                    slaveComm.sendMessage(new TaskMessage(task));
                    // set running in each task processed to one
                    task.running = true;
//                    slaveComm.receiveMessage();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println("Could not send task \n" + task + "\n to slave with SlaveId: " + task.slaveID + ", retrying with different slave if possible");
                    // We remove task from queue and try to reschedule, this is fine because getChillestSlaveId will
                    // not pick the same slaveId again for rescheduling the task that failed
                    taskMap.remove(task.getJobID());
                    List<Task> retryTask = new ArrayList<Task>();
                    retryTask.add(task);
                    scheduleTasks(retryTask);
                }
            }
            // add every task to queue to keep track of them and retry them upon failure
//            taskMap.put(task.getJobID(), task);   // Moved ahead of loop
        }
    }

    /**
     * Master Node receives the task update message that was sent by a slave and sends that to the CoOrdinator
     *
     * @param msg TaskUpdateMessage that was received from a slave indicating completion status of a particular task
     */
    public void processTaskUpdateMessage(TaskUpdateMessage msg) {
        System.out.println("Received message from slave that taskId " + msg.getJobId() + " is done: " + msg.isDone());

        final Task targetTask = taskMap.get(msg.getJobId());
        // if no task exists with that job id then we ignore the message
        if (targetTask == null) {
            return;
        }

        //First before processing for normal circumstances, we check if the message is indicating a failure
        if (!msg.isDone() && !msg.isRunning()) {
            // Task failed on slave, reschedule
            taskMap.remove(targetTask.getJobID());
            scheduleTasks(new ArrayList<Task>() {{
                add(targetTask);
            }});
        }

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

                // Store results in user-defined output file
                if (msg.getPayload() instanceof List) {
                    writeOutputReduceRecords(((ReducerTask) targetTask).getOutputFile(), (List<Record>) msg.getPayload());
                }
            } else if (!msg.isRunning()) {
                // not done, but not running, FAILED on that slave
                int maxRetries = Config.MAX_JOB_RETRIES;
                while (maxRetries > 0) {
                    maxRetries--;
                    break;
                    //TODO (BALA) HANDLE FAILURE of reduce step from slave, send to another slave with a max tries from the config
                }
            }
        }
    }


    /**
     * Return the next Slave ID to run a Task
     * Ideally pick the least burdened slave that contains a local copy of the block
     * desired by a mapper task; if that doesn't work (or you're running for a reducer),
     * just pick the least burdened slave overall
     * <p/>
     * If the slaveId of the current task is given (!= -1), then it is a resassignment upon failure
     * we pick the node using the same algorithm but we cannot choose the node in which the job already failed
     */
    private int getChillestSlaveId(MapperTask mapTask) {
        int minSlave = -1;
        int minJobs = Integer.MAX_VALUE;

        if (mapTask != null) {
            // First try to pick the least burdened slave that locally contains the block we're interested in
            String fileName = mapTask.getDistFile().getFileName();
            int startPosition = mapTask.getDistFile().getStartPosition();
            List<Integer> containingSlaves = nameNode.getSlaveIdsFromPosition(fileName, startPosition);
            for (Integer slaveId : containingSlaves) {
                int numJobs = getRunningTasks(slaveId).size();
                // if (mapTask.getSlaveID() == -1)
                // NEW TASK ASSIGNMENT TO SLAVE
                // never been assigned a slave so we get to choose from all slaveIds
                // else
                // FAILURE REASSIGNMENT, has already been assigned a slave before,
                // We cannot choose the slave where it already failed
                if (numJobs <= minJobs && numJobs <= Config.MAX_TASKS_PER_NODE && mapTask.getSlaveID() != slaveId) {
                    minJobs = numJobs;
                    minSlave = slaveId;
                }
            }
        }

        // Otherwise just pick the least burdened slaveNode
        if (minSlave == -1) {
            for (int slaveId : nameNode.getSlaveIds()) {
                int numJobs = getRunningTasks(slaveId).size();
                // if (mapTask.getSlaveID() == -1)
                // NEW TASK ASSIGNMENT TO SLAVE
                // never been assigned a slave so we get to choose from all slaveIds
                // else
                // FAILURE REASSIGNMENT, has already been assigned a slave before,
                // We cannot choose the slave where it already failed
                if (numJobs < minJobs && (mapTask == null || mapTask.getSlaveID() != slaveId)) {
                    minJobs = numJobs;
                    minSlave = slaveId;
                }
            }
        }

        if (mapTask != null)
            System.out.println("Current chillest slave is " + minSlave + " for position " +
                    mapTask.getDistFile().getStartPosition() + ", so sending task to it");

        return minSlave;
    }

    /**
     * Gives a list of running tasks for a particular Slave
     *
     * @param slaveId Id of slave for which we are requesting running tasks
     * @return List of running tasks for a particular slave node
     */
    public List<Task> getRunningTasks(int slaveId) {
        List<Task> runningTasks = new ArrayList<Task>();
        for (Task task : taskMap.values()) {
            if (task.slaveID == slaveId && task.running) {
                runningTasks.add(task);
            }
        }
        return runningTasks;
    }

    /**
     * Processes a dead slave event
     * Gets dead slaves tasks and re-schedules them
     *
     * @param deadSlaveIds Ids of the slaves that are now DEAD (SLAVE ZOMBIE ALERT)
     */
    public void processDeadSlaveEvent(List<Integer> deadSlaveIds) {
        // REAP TASKS of dead slaves
        List<Task> runningTasks = new ArrayList<Task>();
        for (int deadSlaveId : deadSlaveIds) {
            runningTasks.addAll(getRunningTasks(deadSlaveId));
        }
        for (Task task : runningTasks) {
            task.running = false;
            taskMap.remove(task.getJobID());
        }
        // Reschedule them
        scheduleTasks(runningTasks);
    }


    /**
     * Take a Reducer task and send it to the next available SlaveNode to be run
     *
     * @param reducerTask Reduce task to be run
     */
    public void scheduleReducer(ReducerTask reducerTask) {
        reducerTask.slaveID = getChillestSlaveId(null);

        // send tasks to slave to begin processing
        try {
            CommHandler slaveComm = new CommHandler(Config.SLAVE_NODES[reducerTask.slaveID][0],
                    Config.SLAVE_NODES[reducerTask.slaveID][1]);
            slaveComm.sendMessage(new TaskMessage(reducerTask));
            // set running in each task processed to one
            reducerTask.running = true;
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Could not send task \n" + reducerTask + "\n to slave with SlaveId: " + reducerTask.slaveID + ", retrying with different slave if possible");
            // We remove task from queue and try to reschedule, this is fine because getChillestSlaveId will
            // not pick the same slaveId again for rescheduling the task that failed
            taskMap.remove(reducerTask.getJobID());
            List<Task> retryTask = new ArrayList<Task>();
            retryTask.add(reducerTask);
            scheduleTasks(retryTask);
        }

    }

    /**
     * Appends result records from Reducer to the specified output file
     *
     * @param outputFileName      Name of file to append to
     * @param finalReducerResults List of result records
     */
    public void writeOutputReduceRecords(String outputFileName, List<Record> finalReducerResults) {
        // Append to output file
        try {
            FileWriter fw = new FileWriter(outputFileName, true);
            for (Record record : finalReducerResults) {
                fw.append(record.getKey() + "\t" + record.getValue() + "\n");
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Wrote " + finalReducerResults.size() + " result records to " + outputFileName + "!");
    }
}
