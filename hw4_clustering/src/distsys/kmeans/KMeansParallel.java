package distsys.kmeans;

import distsys.datagen.DataGenerator;
import distsys.datagen.Point2DGenerator;
import distsys.kmeans.datapoints.Point2D;
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

    List<DataPoint> fullDataPoints = new ArrayList<DataPoint>();

    /**
     * Class for performing a K-Means clustering algorithm across a set of DataPoints
     *
     * @param numPoints   Number of DataPoints
     * @param numClusters Number of Clusters to find
     */
    public KMeansParallel(int numPoints, int numClusters, String[] args) {
        super(numPoints, numClusters);

        // Set up MPI
        try {
            MPI.Init(args);
        } catch (MPIException e) {
            System.err.println("Something broke initializing MPI (" + e.getMessage() + ").");
        }
    }

    private void generateData() {
        fullDataPoints.add(new Point2D(2, 2));
        fullDataPoints.add(new Point2D(-1, 3.5));
        fullDataPoints.add(new Point2D(3, 1));
        fullDataPoints.add(new Point2D(-1, -2));
        fullDataPoints.add(new Point2D(-2, 2.5));
        fullDataPoints.add(new Point2D(1, -1));
        fullDataPoints.add(new Point2D(3, 3.5));
        fullDataPoints.add(new Point2D(-3, 3));
        fullDataPoints.add(new Point2D(4, 3));
        fullDataPoints.add(new Point2D(2, -2));
    }

    /**
     * Set up the initial Centroids and distribute partitions of the DataPoints to each processor
     *
     * @param myRank The rank of this processor
     */
    private void initializeDataPoints(int myRank, int totalHosts) throws MPIException {
        if (myRank == ROOT) {
            // Split up the data between hosts
            centroids = chooseInitialCentroids(fullDataPoints);
            sendInitDataPoints(fullDataPoints, totalHosts);
        } else {
            // Receive yo datapoints from the root host
            receiveInitDataPoints(myRank);
        }
    }


    /**
     * Split the set of DataPoints into equal chunks and send them each to their own processor
     *
     * @param allData  A list of every available DataPoint
     * @param numHosts Number of processors to split into
     */
    private void sendInitDataPoints(List<DataPoint> allData, int numHosts) {
        // First set DataPoints for the root host
        int partition = allData.size() / numHosts;
        dataPoints.addAll(allData.subList(0, partition));
        for (int i = 1; i < numHosts; i++) {
            // Convert hosts datapoints to byte array and send them over
            List<DataPoint> hostPoints = new ArrayList<DataPoint>();
            if (i < numHosts - 1) {
                // Add a chunk of datapoints for this processor
                hostPoints.addAll(allData.subList(partition * i, partition * (i + 1)));
            } else {
                // Scoop up remainder of data at the end
                hostPoints.addAll(allData.subList(partition * i, allData.size()));
            }

            try {
                // Send the subset of datapoints to its processor
                byte[] dataPointBytes = serialize(hostPoints);
                MPI.COMM_WORLD.send(dataPointBytes, dataPointBytes.length, MPI.BYTE, i, TAG_SEND_DATA);

                // Send the initial centroids to a processor
                byte[] centroidBytes = serialize(centroids);
                MPI.COMM_WORLD.send(centroidBytes, centroidBytes.length, MPI.BYTE, i, TAG_SEND_CENT);
            } catch (MPIException e) {
                System.err.println("Error sending DataPoints to host " + i + " (" + e.getMessage() + ").");
            }
        }
    }

    /**
     * Receive a partition of DataPoints from the root host
     *
     * @param myRank Rank of the processor to receive DataPoints
     * @throws MPIException
     */
    private void receiveInitDataPoints(int myRank) throws MPIException {
        // Receive initial DataPoints
        System.out.print("Host " + myRank + " is waiting to receive DataPoints..\n");
        int numBytesDataPoints = MPI.COMM_WORLD.probe(ROOT, TAG_SEND_DATA).getCount(MPI.BYTE);
        byte[] dataPointBytes = new byte[numBytesDataPoints];
        MPI.COMM_WORLD.recv(dataPointBytes, numBytesDataPoints, MPI.BYTE, ROOT, TAG_SEND_DATA);
        dataPoints = (List<DataPoint>) deserialize(dataPointBytes);

        // Receive initial Centroids
        int numBytesCentroid = MPI.COMM_WORLD.probe(ROOT, TAG_SEND_CENT).getCount(MPI.BYTE);
        byte[] centroidBytes = new byte[numBytesCentroid];
        MPI.COMM_WORLD.recv(centroidBytes, numBytesCentroid, MPI.BYTE, ROOT, TAG_SEND_CENT);
        centroids = (List<Centroid>) deserialize(centroidBytes);

        System.out.print("Host " + myRank + " received " + dataPoints.size() + " datapoints and " +
                centroids.size() + " centroids!!\n");
    }

    /**
     * Collect all the clustered DataPoints on the root processor
     *
     * @param myRank Rank of the processor to receive DataPoints
     */
    private void collectResults(int myRank, int totalHosts) throws MPIException {
        if (myRank == ROOT) {
            // Receive datapoints from everyone!
            System.out.println("Root host is receiving clustered datapoints from everyone else..");
            for (int i = 1; i < totalHosts; i++) {
                // Receive list bytes from a host
                int numDataPointBytes = MPI.COMM_WORLD.probe(i, TAG_SEND_DATA).getCount(MPI.BYTE);
                byte[] dataPointBytes = new byte[numDataPointBytes];
                MPI.COMM_WORLD.recv(dataPointBytes, numDataPointBytes, MPI.BYTE, i, TAG_SEND_DATA);
                List<DataPoint> newDataPoints = (List<DataPoint>) deserialize(dataPointBytes);

                // Add the new points to the ones already stored
                dataPoints.addAll(newDataPoints);
            }
            System.out.println("Root received datapoints from " + (totalHosts - 1) + " hosts!");

            for (DataPoint d : dataPoints) {
                System.out.print(d + "\n");
            }
        } else {
            // Send the subset of datapoints to its processor
            byte[] dataPointBytes = serialize(dataPoints);
            MPI.COMM_WORLD.send(dataPointBytes, dataPointBytes.length, MPI.BYTE, ROOT, TAG_SEND_DATA);
        }
    }


    /**
     * Execute the K-Means clustering algorithm, but in parallel with OpenMPI
     */
    @Override
    public void findClusters() {
        try {
            // Randomly select initial centroids
            int iterations = 0;
            int[] numPointsChanged = new int[]{Integer.MAX_VALUE};

            int myRank = MPI.COMM_WORLD.getRank();
            int totalHosts = MPI.COMM_WORLD.getSize();
            System.out.print("Running k-means with rank " + myRank + " of " + totalHosts + ".\n");

            // For the ROOT processor, set the initial centroids and send everyone their portion of the DataPoints
            initializeDataPoints(myRank, totalHosts);
            MPI.COMM_WORLD.barrier();  // Wait here until everyone's ready

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
                    System.out.print("(P" + myRank + ") DataPoint " + point + "\n");       //TODO remove
                }

                // Calculate the new centroids for all clusters
                centroids = centroids.get(0).calculateAllCentroidsParallel(dataPoints, numClusters);

                MPI.COMM_WORLD.allReduce(numPointsChanged, 1, MPI.INT, MPI.SUM);
                if (myRank == ROOT)
                    System.out.print("Iteration " + iterations + ": changed " + numPointsChanged[0] + " points.\n");
            }

            // Done clustering, collect the results from the other hosts
            collectResults(myRank, totalHosts);

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


    /**
     * Main Method
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) throws MPIException {
        KMeansParallel kmeans = new KMeansParallel(20, 3, args);

        // Generate data points
        if (MPI.COMM_WORLD.getRank() == 0) {
            DataGenerator dataGen = new Point2DGenerator(20, 3);
            List<DataPoint> dataPoints = dataGen.generatePoints();
            kmeans.setFullDataPoints(dataPoints);
        }

        kmeans.findClusters();
//        ClusterMain.writeResultsToFile(kmeans.getDataPoints(), kmeans.numClusters);
    }

    public void setFullDataPoints(List<DataPoint> fullDataPoints) {
        this.fullDataPoints = fullDataPoints;
    }
}
