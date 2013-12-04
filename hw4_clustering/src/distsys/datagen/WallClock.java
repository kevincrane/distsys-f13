package distsys.datagen;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/4/13
 */
public class WallClock {

    private long startTime;
    private boolean hasStarted;

    /**
     * Used to time program execution; startTimer() -> getRunTime()
     */
    public WallClock() {
        hasStarted = false;
    }

    /**
     * Start the program timer
     */
    public void startTimer() {
        hasStarted = true;
        startTime = System.currentTimeMillis();
    }

    /**
     * Return the current time elapsed through the timer in milliseconds
     *
     * @return The current time elapsed
     */
    public long getRunTime() {
        long stopTime = System.currentTimeMillis();
        if (!hasStarted) {
            System.err.println("WallClock hasn't been started yet.");
            return -1;
        }
        return stopTime - startTime;
    }

}
