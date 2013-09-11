package distsys.process;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/8/13
 */
import distsys.io.TransactionalFileInputStream;
import distsys.io.TransactionalFileOutputStream;

import java.io.PrintStream;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.Thread;
import java.lang.InterruptedException;

public class GrepProcess implements MigratableProcess {
    private String processName;
    private TransactionalFileInputStream inFile;
    private TransactionalFileOutputStream outFile;
    private String query;
    private String inFileName;
    private String outFileName;

    private volatile boolean suspending;

    public GrepProcess(String args[]) throws Exception
    {
        if (args.length != 3) {
            System.out.println("usage: GrepProcess <queryString> <inputFile> <outputFile>");
            throw new Exception("Invalid Arguments");
        }

        query = args[0];
        inFileName = args[1];
        outFileName = args[2];

        //TODO: fix this
//        inFile = new TransactionalFileInputStream(args[1]);
//        outFile = new TransactionalFileOutputStream(args[2], false);
    }

    @Override
    public void run() {
        PrintStream out = new PrintStream(outFile);
        DataInputStream in = new DataInputStream(inFile);

        try {
            while (!suspending) {
                String line = in.readLine();

                if (line == null) break;

                if (line.contains(query)) {
                    out.println(line);
                }

                // Make grep take longer so that we don't require extremely large files for interesting results
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore it
                }
            }
        } catch (EOFException e) {
            //End of File
        } catch (IOException e) {
            System.out.println ("GrepProcess: Error: " + e);
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
        return "Process[GrepProcess " + query + " " + inFileName + " " + outFileName + "]";
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