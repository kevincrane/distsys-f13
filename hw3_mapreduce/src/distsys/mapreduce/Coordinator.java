package distsys.mapreduce;

import distsys.kdfs.NameNode;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
        for (Task task : tasks) {
            // assign correct slaveIds for each task
            // send tasks to slaves to begin processing, set running in each task processed to one
            System.out.println("Recevied Task to schedule: \n" + task);
        }
        taskQueue.addAll(tasks);
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
