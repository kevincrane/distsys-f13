package distsys.kmeans;

import mpi.MPI;
import mpi.MPIException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/5/13
 */
public class KMeansParallel extends KMeans {
    private final int TAG_SEND_DATA = 12;
    private final int TAG_SEND_CENT = 13;
    private final int ROOT = 0;
    private final int myRank;
    private final int totalHosts;

    List<DataPoint> fullDataPoints = new ArrayList<DataPoint>();

    /**
     * Class for performing a K-Means clustering algorithm across a set of DataPoints
     *
     * @param numPoints   Number of DataPoints
     * @param numClusters Number of Clusters to find
     */
    public KMeansParallel(int numPoints, int numClusters) throws MPIException {
        super(numPoints, numClusters);

        // Set up MPI
        MPI.Init(new String[]{});
        this.myRank = MPI.COMM_WORLD.Rank();
        this.totalHosts = MPI.COMM_WORLD.Size();
    }


    /**
     * Set up the initial Centroids and distribute partitions of the DataPoints to each processor
     */
    private void initializeDataPoints() throws MPIException {
        if (isRoot()) {
            // Split up the data between hosts
            centroids = chooseInitialCentroids(fullDataPoints);
            sendInitDataPoints(fullDataPoints);
        } else {
            // Receive yo datapoints from the root host
            receiveInitDataPoints();
        }
    }


    /**
     * Split the set of DataPoints into equal chunks and send them each to their own processor
     *
     * @param allData A list of every available DataPoint
     */
    private void sendInitDataPoints(List<DataPoint> allData) {
        // First set DataPoints for the root host
        int partition = allData.size() / totalHosts;
        dataPoints.addAll(allData.subList(0, partition));
        for (int i = 1; i < totalHosts; i++) {
            // Convert hosts datapoints to byte array and send them over
            List<DataPoint> hostPoints = new ArrayList<DataPoint>();
            if (i < totalHosts - 1) {
                // Add a chunk of datapoints for this processor
                hostPoints.addAll(allData.subList(partition * i, partition * (i + 1)));
            } else {
                // Scoop up remainder of data at the end
                hostPoints.addAll(allData.subList(partition * i, allData.size()));
            }

            try {
                // Send the subset of datapoints to its processor
                byte[] dataPointBytes = serialize(hostPoints);
                MPI.COMM_WORLD.Send(dataPointBytes, 0, dataPointBytes.length, MPI.BYTE, i, TAG_SEND_DATA);

                // Send the initial centroids to a processor
                byte[] centroidBytes = serialize(centroids);
                MPI.COMM_WORLD.Send(centroidBytes, 0, centroidBytes.length, MPI.BYTE, i, TAG_SEND_CENT);
            } catch (MPIException e) {
                System.err.println("Error sending DataPoints to host " + i + " (" + e.getMessage() + ").");
            }
        }
    }

    /**
     * Receive a partition of DataPoints from the root host
     *
     * @throws mpi.MPIException
     */
    @SuppressWarnings("unchecked")
    private void receiveInitDataPoints() throws MPIException {
        // Receive initial DataPoints
        System.out.print("Host " + myRank + " is waiting to receive DataPoints..\n");
        int numBytesDataPoints = MPI.COMM_WORLD.Probe(ROOT, TAG_SEND_DATA).Get_count(MPI.BYTE);
        byte[] dataPointBytes = new byte[numBytesDataPoints];
        MPI.COMM_WORLD.Recv(dataPointBytes, 0, numBytesDataPoints, MPI.BYTE, ROOT, TAG_SEND_DATA);
        dataPoints = (List<DataPoint>) deserialize(dataPointBytes);

        // Receive initial Centroids
        int numBytesCentroid = MPI.COMM_WORLD.Probe(ROOT, TAG_SEND_CENT).Get_count(MPI.BYTE);
        byte[] centroidBytes = new byte[numBytesCentroid];
        MPI.COMM_WORLD.Recv(centroidBytes, 0, numBytesCentroid, MPI.BYTE, ROOT, TAG_SEND_CENT);
        centroids = (List<Centroid>) deserialize(centroidBytes);

        System.out.print("Host " + myRank + " received " + dataPoints.size() + " datapoints and " +
                centroids.size() + " centroids!!\n");
    }

    /**
     * Collect all the clustered DataPoints on the root processor
     */
    @SuppressWarnings("unchecked")
    private void collectResults() throws MPIException {
        if (isRoot()) {
            // Receive datapoints from everyone!
            System.out.println("Root host is collecting clustered datapoints from other hosts..");
            for (int i = 1; i < totalHosts; i++) {
                // Receive list bytes from a host
                int numDataPointBytes = MPI.COMM_WORLD.Probe(i, TAG_SEND_DATA).Get_count(MPI.BYTE);
                byte[] dataPointBytes = new byte[numDataPointBytes];
                MPI.COMM_WORLD.Recv(dataPointBytes, 0, numDataPointBytes, MPI.BYTE, i, TAG_SEND_DATA);
                List<DataPoint> newDataPoints = (List<DataPoint>) deserialize(dataPointBytes);

                // Add the new points to the ones already stored
                dataPoints.addAll(newDataPoints);
            }

//            for (DataPoint d : dataPoints) {      //TODO for debugging
//                System.out.print(d + "\n");
//            }
        } else {
            // Send the subset of datapoints to its processor
            byte[] dataPointBytes = serialize(dataPoints);
            MPI.COMM_WORLD.Send(dataPointBytes, 0, dataPointBytes.length, MPI.BYTE, ROOT, TAG_SEND_DATA);
        }
    }


    /**
     * Execute the K-Means clustering algorithm, but in parallel with OpenMPI
     */
    @Override
    public void findClusters() {
        if (isRoot())
            System.out.println("Forming " + numClusters + " clusters from " + numPoints + " datapoints using K-Means..");
        try {
            // Randomly select initial centroids
            int iterations = 0;
            int[] numPointsChanged = new int[]{Integer.MAX_VALUE};

            System.out.print("Running k-means with rank " + myRank + " of " + totalHosts + ".\n");

            // For the ROOT processor, set the initial centroids and send everyone their portion of the DataPoints
            initializeDataPoints();
            MPI.COMM_WORLD.Barrier();  // Wait here until everyone's ready

            // ACTUAL K-MEANS CLUSTERING
            // Repeat until the number of points that changes clusters exceeds a certain threshold (0.1% of points)
            while ((((double) numPointsChanged[0]) / numPoints) > THRESHOLD && iterations < MAX_ITERATIONS) {
                numPointsChanged[0] = 0;
                iterations++;

                for (DataPoint point : dataPoints) {
                    // Find the nearest centroid for each DataPoint
                    int closestCluster = getClosestCluster(point, centroids);

                    // Update the DataPoint's cluster if it changed
                    if (point.getCluster() != closestCluster) {
                        point.setCluster(closestCluster);
                        numPointsChanged[0]++;
                    }
//                    System.out.print("(P" + myRank + ") DataPoint " + point + "\n");       //TODO for debugging
                }

                // Calculate the new centroids for all clusters and find total number of changed datapoints
                centroids = centroids.get(0).calculateAllCentroidsParallel(dataPoints, numClusters);
                MPI.COMM_WORLD.Allreduce(numPointsChanged, 0, numPointsChanged, 0, 1, MPI.INT, MPI.SUM);
                if (isRoot()) {
                    for (Centroid c : centroids) {
                        System.out.println("New centroid location " + c);
                    }
                    System.out.println("Iteration " + iterations + ": changed " + numPointsChanged[0] + " points.\n");
                }
            }

            // Done clustering, collect the results from the other hosts
            collectResults();

        } catch (MPIException e) {
            System.err.println("Error: something broke with MPI (" + e.getMessage() + ")");
        } finally {
            try {
                MPI.Finalize();
            } catch (MPIException ignored) {
            }
        }
    }

    /**
     * Turn an object into a byte array
     * Lovingly referenced from: http://stackoverflow.com/questions/5837698/converting-any-object-to-a-byte-array-in-java
     */
    public static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream o = new ObjectOutputStream(b);
            o.writeObject(obj);
            return b.toByteArray();
        } catch (IOException e) {
            System.err.println("Error: couldn't serialize object (" + e.getMessage() + ").");
        }
        return null;
    }

    /**
     * Turn a byte array into an object
     * Lovingly referenced from: http://stackoverflow.com/questions/5837698/converting-any-object-to-a-byte-array-in-java
     */
    public static Object deserialize(byte[] bytes) {
        try {
            ByteArrayInputStream b = new ByteArrayInputStream(bytes);
            ObjectInputStream o = new ObjectInputStream(b);
            return o.readObject();
        } catch (IOException e) {
            System.err.println("Error: couldn't serialize byte array (" + e.getMessage() + ").");
        } catch (ClassNotFoundException e) {
            System.err.println("Error: couldn't serialize byte array (" + e.getMessage() + ").");
        }
        return null;
    }


    public void setFullDataPoints(List<DataPoint> fullDataPoints) {
        this.fullDataPoints = fullDataPoints;
    }

    public boolean isRoot() {
        return myRank == ROOT;
    }
}
