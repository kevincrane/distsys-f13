package distsys.process;

import distsys.io.TransactionalFileInputStream;
import distsys.io.TransactionalFileOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/8/13
 */

public class FileCountProcess implements MigratableProcess {
    private String processName;
    private File file;
    private int maxValue;
    private volatile boolean suspending;

    public FileCountProcess(String args[]) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: FileCountProcess <maxValue> <fileName>");
            throw new Exception("Invalid Arguments");
        }

        try {
            maxValue = Integer.parseInt(args[0]);
            file = new File(args[1]);
            if (!file.exists()) {
                System.out.println("<fileName> has to be a valid existing file. Please check your path.");
                throw new Exception("Invalid filename");
            }
        } catch(NumberFormatException e) {
            System.out.println("<maxValue> must be an integer.");
            throw new Exception("Invalid number");
        }
    }

    @Override
    public void run() {
        while (!suspending) {
            try {
                TransactionalFileInputStream in = new TransactionalFileInputStream(file);
                TransactionalFileOutputStream out = new TransactionalFileOutputStream(file);

                int i;
                for (i = 1; i <= maxValue; i++) {
                    out.write(i);
                    System.out.println("Writing number: " + i);
                    // Make count take longer so that we don't require extremely large numbers for interesting results
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore it
                    }
                }
                i = in.read();
                while (i > 0) {
                    System.out.println(i);
                    i = in.read();
                    // Make count take longer so that we don't require extremely large numbers for interesting results
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore it
                    }
                }
                break;
            } catch (FileNotFoundException e) {
                System.out.println("<fileName> has to be a valid existing file. Please check your path.");
                // this shouldn't happen
            } catch (IOException e) {
                System.out.println("An IO error happened while reading or writing file");
            }
        }

        suspending = false;
    }

    @Override
    public void suspend() {
        suspending = true;
        while (suspending);
    }

    @Override
    public String toString() {
        return "Process[FileCountProcess " + maxValue + "]";
    }

    @Override
    public String getProcessName() {
        return processName;
    }

    @Override
    public void setProcessName(String processName) {
        this.processName = processName.replace(" ", "_");
    }

}