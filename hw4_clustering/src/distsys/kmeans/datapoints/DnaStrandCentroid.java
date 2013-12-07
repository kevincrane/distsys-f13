package distsys.kmeans.datapoints;

import distsys.kmeans.Centroid;
import distsys.kmeans.DataPoint;
import mpi.MPI;
import mpi.MPIException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/4/13
 */
public class DnaStrandCentroid extends DnaStrand implements Centroid {

    /**
     * DataPoint that represents a DNA strand
     *
     * @param strand List of nucleotide bases that make up the strand
     */
    public DnaStrandCentroid(Bases[] strand, int cluster) {
        super(strand);
        this.cluster = cluster;
    }

    /**
     * Calculate the distance between two DnaStrands, equal to the number of nucleotides that differ between them.
     * For this, we assume that both strands are the same length (could break otherwise);
     * TODO: To improve this in the future, switch this simple version with the Levenshtein Distance
     *
     * @param point The DnaStrand to be compared to the current one
     * @return The distance between this centroid and the other DnaStrand
     */
    @Override
    public double distanceFrom(DataPoint point) {
        if (!(point instanceof DnaStrand)) {
            System.err.println("Error: can't compare distance between types DnaStrand and " + point.getClass().getName());
            return -1;
        }

        int distance = 0;
        for (int i = 0; i < strand.length; i++) {
            if (strand[i] != ((DnaStrand) point).getStrand()[i]) {
                distance++;
            }
        }
        return distance;
    }

    /**
     * Compute a new centroid of all points in this cluster.
     * Find most common nucleotide in each position of the strand for every position
     *
     * @param points A list of all DnaStrands to be considered
     */
    @Override
    public void calculateNewCenter(List<DataPoint> points) {
        // Initialize map to count bases in each position
        Map<Bases, List<AtomicInteger>> baseCount = new HashMap<Bases, List<AtomicInteger>>();
        for (Bases base : Bases.values()) {
            List<AtomicInteger> counter = new ArrayList<AtomicInteger>(strand.length);
            for (Bases aStrand : strand) {
                counter.add(new AtomicInteger());
            }
            baseCount.put(base, counter);
        }

        for (DataPoint point : points) {
            // First typecheck the DataPoint to be sure it's a DnaStrand
            if (!(point instanceof DnaStrand)) {
                continue;
            }

            // If this point belongs to the right cluster, count its bases
            if (point.getCluster() == cluster) {
                // Iterate the nucleotide count for each position in this DnaStrand
                DnaStrand nextStrand = (DnaStrand) point;
                for (int i = 0; i < nextStrand.getStrand().length; i++) {
                    baseCount.get(nextStrand.getStrand()[i]).get(i).incrementAndGet();
                }
            }
        }

        // Find most common nucleotide in each position and set it for this centroid
        for (int i = 0; i < strand.length; i++) {
            Bases mostCommonBase = null;
            int mostCommonCount = Integer.MIN_VALUE;

            // Look through each base to see which was most common at this position
            for (Bases base : baseCount.keySet()) {
                if (baseCount.get(base).get(i).get() > mostCommonCount) {
                    // I wish Java let me get more gets. 3 isn't enough
                    mostCommonCount = baseCount.get(base).get(i).get();
                    mostCommonBase = base;
                }
            }
            strand[i] = mostCommonBase;
        }
        System.out.println("New centroid location " + this);
    }

    @Override
    public List<Centroid> calculateAllCentroidsParallel(List<DataPoint> points, int numClusters) {
        List<Centroid> newCentroids = new ArrayList<Centroid>(numClusters);
        int[][] totalA = new int[numClusters][strand.length]; // cluster -> # of base at a pos
        int[][] totalG = new int[numClusters][strand.length];
        int[][] totalC = new int[numClusters][strand.length];
        int[][] totalT = new int[numClusters][strand.length];

        // Create sum of each nucleotide at each position for every cluster
        for (DataPoint point : points) {
            for (int i = 0; i < ((DnaStrand) point).getStrand().length; i++) {
                // This part is dumb; OpenMPI documentation was sparse, I swear that's part of the reason
                switch (((DnaStrand) point).getStrand()[i]) {
                    case A:
                        totalA[point.getCluster()][i]++;
                        break;
                    case G:
                        totalG[point.getCluster()][i]++;
                        break;
                    case C:
                        totalC[point.getCluster()][i]++;
                        break;
                    case T:
                        totalT[point.getCluster()][i]++;
                        break;
                }
            }
        }

        try {
            // Reduce Sum the base count for each strand position in each cluster (could almost certainly be optimized)
            for (int i = 0; i < numClusters; i++) {
                MPI.COMM_WORLD.allReduce(totalA[i], strand.length, MPI.INT, MPI.SUM);
                MPI.COMM_WORLD.allReduce(totalG[i], strand.length, MPI.INT, MPI.SUM);
                MPI.COMM_WORLD.allReduce(totalC[i], strand.length, MPI.INT, MPI.SUM);
                MPI.COMM_WORLD.allReduce(totalT[i], strand.length, MPI.INT, MPI.SUM);
            }

            // Find most common nucleotide in each position and define a new centroid for each cluster
            for (int i = 0; i < numClusters; i++) {
                Bases[] newStrand = new Bases[strand.length];
                for (int j = 0; j < strand.length; j++) {
                    Bases mostCommonBase = Bases.A;
                    int mostCommonCount = totalA[i][j];

                    // Dumbest possible way to find the max but OpenMPI docs suck and now I have awful data structures
                    if (totalG[i][j] > mostCommonCount) {
                        mostCommonBase = Bases.G;
                        mostCommonCount = totalG[i][j];
                    }
                    if (totalC[i][j] > mostCommonCount) {
                        mostCommonBase = Bases.C;
                        mostCommonCount = totalC[i][j];
                    }
                    if (totalT[i][j] > mostCommonCount) {
                        mostCommonBase = Bases.T;
                    }
                    newStrand[j] = mostCommonBase;
                }

                newCentroids.add(new DnaStrandCentroid(newStrand, i));
            }
            return newCentroids;
        } catch (MPIException e) {
            System.err.println("Error during reducing point to find new centroid");
            return null;
        }
    }
}
