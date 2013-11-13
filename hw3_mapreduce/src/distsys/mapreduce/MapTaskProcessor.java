package distsys.mapreduce;

import distsys.Config;
import distsys.msg.CommHandler;
import distsys.msg.TaskUpdateMessage;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 * Time: 3:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class MapTaskProcessor extends TaskProcessor {
    private MapperTask task;

    public MapTaskProcessor(MapperTask mapperTask, CommHandler commHandler) {
        super(mapperTask, commHandler);
        this.task = mapperTask;
    }

    public void run() {
        Mapper mapper = task.getMapper();

        System.out.println("Got Map Task in Slave: " + task);
        // DO MAP
        //TODO MAPPER JOB FILE IS NAMED SO IT CAN BE FETCHED BY JOBID  mpr_<JOBID>
        //TODO HANDLE FAILURE BY SENDING MASTER NO MESSAGES OR BY SENDING ISrunning false, IsDONE false, message

        // DONE MAP

        // We are DONE, TELL MASTER we are done, we retry in case of failure for MAX_SOCKET_TRIES times
        int maxTries = Config.MAX_SOCKET_TRIES;
        while (maxTries > 0) {
            try {
                getMasterComm().sendMessage(new TaskUpdateMessage(task.getJobID(), false, true));
                System.out.println("Sent message to master. Map Job with Id: " + task.getJobID() + " is done.");
                break;
            } catch (IOException e) {
                maxTries--;
                e.printStackTrace();
                if (maxTries == 0)  {
                    System.err.println("ERROR: Could not send any messages to master, master is down.");
                }
            }
        }

    }
}
