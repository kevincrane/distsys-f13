package distsys.kmeans.datapoints;

import distsys.kmeans.Centroid;
import distsys.kmeans.DataPoint;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 12/4/13
 */
public class DnaStrand extends DataPoint {

    public enum Bases {
        A, G, C, T;
    }

    Bases[] strand;

    /**
     * DataPoint that represents a DNA strand
     *
     * @param strand List of nucleotide bases that make up the strand
     */
    public DnaStrand(Bases[] strand) {
        super(-1);
        this.strand = strand;
    }

    @Override
    public Centroid dataPointToCentroid(int cluster) {
        return new DnaStrandCentroid(strand, cluster);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(cluster);
        sb.append(" - ");
        for (Bases base : strand) {
            sb.append(base);
        }
        return sb.toString();
    }

    public Bases[] getStrand() {
        return strand;
    }
}
