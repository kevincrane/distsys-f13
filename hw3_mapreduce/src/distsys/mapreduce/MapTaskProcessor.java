package distsys.mapreduce;

import distsys.Config;
import distsys.kdfs.DistFile;
import distsys.msg.CommHandler;
import distsys.msg.TaskUpdateMessage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 11/13/13
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
        DistFile distFile = task.getDistFile();
        Record<Integer, String> currentRecord;

        // Run map(key, value) for every record in the block
        while ((currentRecord = distFile.nextRecord()) != null) {
            mapper.map(currentRecord.getKey(), currentRecord.getValue());
        }

        // Write output of mapper to temp file
        List<Record> mapOutput = mapper.getMapOutput();
        String resultFileName = String.format("%s%d%03d", Config.MAP_RESULTS, task.slaveID, task.getJobID());   //TODO here
        File outputFile = new File(resultFileName);
        boolean isDone = false;
        try {
            FileWriter fw = new FileWriter(outputFile);
            for (Record record : mapOutput) {
                fw.write(record.getKey() + "\t" + record.getValue() + "\n");
            }
            fw.close();
            isDone = true;
        } catch (IOException e) {
            e.printStackTrace();
            //Could not complete, the TaskUpdateMessage, will have isDone set to false as it should
        }

        // We are DONE, TELL MASTER we are done, we retry in case of failure for MAX_SOCKET_TRIES times
        int triesLeft = Config.MAX_SOCKET_TRIES;
        while (triesLeft > 0) {
            try {
                CommHandler masterComm = new CommHandler(Config.MASTER_NODE, Config.DATA_PORT);
                masterComm.sendMessage(new TaskUpdateMessage(task.getJobID(), false, isDone));
//                getMasterComm().sendMessage(new TaskUpdateMessage(task.getJobID(), false, isDone));
                System.out.println("Sent message to master. Map Job with Id: " + task.getJobID() + " is done.");
                break;
            } catch (IOException e) {
                triesLeft--;
                e.printStackTrace();
                if (triesLeft == 0) {
                    System.err.println("ERROR: Could not send any messages to master, master is down.");
                }
            }
        }

    }
}
