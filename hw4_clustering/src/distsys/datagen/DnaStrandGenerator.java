package distsys.datagen;

import distsys.kmeans.DataPoint;
import distsys.kmeans.datapoints.DnaStrand;
import distsys.kmeans.datapoints.DnaStrandCentroid;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/4/13
 */
public class DnaStrandGenerator extends DataGenerator {

    private final int MAX_CHANGES;
    private final double MIN_DIST;
    private List<DnaStrand> initPoints;
    private int strandLength;

    public DnaStrandGenerator(int numPoints, int numClusters, int strandLength) {
        super(numPoints, numClusters);
        initPoints = new ArrayList<DnaStrand>(numClusters);
        this.strandLength = strandLength;
        this.MAX_CHANGES = (int) (strandLength * 0.5);
        this.MIN_DIST = strandLength * 0.5;     //TODO is this value good?
    }


    /**
     * Generate initial random centroid starting spots
     */
    void setRandomCentroids() {
        boolean tooNear;
        for (int i = 0; i < numClusters; i++) {
            tooNear = false;
            // Generate random position for initial centroid (at least MAX_CHANGES away from the plane borders)
            DnaStrand.Bases[] centBases = new DnaStrand.Bases[strandLength];
            for (int j = 0; j < centBases.length; j++) {
                centBases[j] = randomBase();
            }
            DnaStrandCentroid newInitPoint = new DnaStrandCentroid(centBases, i);

            // Make sure no other centroid is too nearby
            for (DnaStrand point : initPoints) {
                if (newInitPoint.distanceFrom(point) < MIN_DIST) {
                    i--;
                    tooNear = true;
                    break;
                }
            }

            // Add to list of initial centroids
            if (!tooNear) {
                initPoints.add(newInitPoint);
                System.out.println("Set initial centroid at " + newInitPoint);
            }
        }
    }


    /**
     * Generate a series of random DnaStrands with random number of modifications around clusters (up to MAX_CHANGES)
     *
     * @return List of DataPoints
     */
    @Override
    public List<DataPoint> generatePoints() {
        System.out.println("Generating random Point2D data clusters..");
        List<DataPoint> randomPoints = new ArrayList<DataPoint>(numPoints);

        // Set initial random centroids
        setRandomCentroids();

        // Generate random mutations in each centroid DNA strand
        for (DnaStrand cent : initPoints) {
            for (int i = 0; i < Math.ceil(((double) numPoints) / numClusters) && randomPoints.size() < numPoints; i++) {
                int numMutations = rand.nextInt(MAX_CHANGES);
                DnaStrand.Bases[] centroidStrand = cent.getStrand().clone();

                // Create random mutations in the DNA Strand
                for (int j = 0; j < numMutations; j++) {
                    centroidStrand[rand.nextInt(centroidStrand.length)] = randomBase();
                }
                randomPoints.add(new DnaStrand(centroidStrand));
            }
        }

        System.out.println("Generated " + randomPoints.size() + " random points in " + numClusters + " clusters!\n");
        return randomPoints;
    }

    /**
     * Return a random DNA nucleotide base
     */
    private DnaStrand.Bases randomBase() {
        return DnaStrand.Bases.values()[rand.nextInt(DnaStrand.Bases.values().length)];
    }

}
