package distsys.process;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 9/8/13
 */

public class CountProcess implements MigratableProcess {
    private int maxValue;
    private int currentValue;
    private volatile boolean suspending;

    public CountProcess(String args[]) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: CountProcess <maxValue>");
            throw new Exception("Invalid Arguments");
        }

        try {
            maxValue = Integer.parseInt(args[0]);
        } catch(NumberFormatException e) {
            System.out.println("<maxValue> must be an integer.");
            throw new Exception("Invalid Arguments");
        }
        currentValue = 0;
    }

    @Override
    public void run() {
        while (!suspending) {
            System.out.println(currentValue);
            currentValue++;
            if(currentValue > maxValue) {
                System.out.println("Done!");
                break;
            }

            // Make count take longer so that we don't require extremely large numbers for interesting results
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore it
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
        return "Process[CountProcess " + maxValue + "]";
    }

}