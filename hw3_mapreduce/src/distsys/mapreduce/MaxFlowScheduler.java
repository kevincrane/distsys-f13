package distsys.mapreduce;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 11/13/13
 * Time: 11:05 PM
 * To change this template use File | Settings | File Templates.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Alternative advanced scheduler, which models the scheudling problem as a max flow problem as described below and
 * gives the optimal solution for scheduling by running the Ford Fulkerson algorithm
 * Modelling scheudling as a max flow problem
 * A Bipartite graph with jobs on one side connected to nodes on it's blocks are stored on the other side.
 * With edges of capacity 1 from the SOURCE, connecting each job, each job connects with edges of infinite capacity to
 * the nodes on the other side representing the nodes. Nodes then connect to the SINK with capacity equal to the number
 * of cores on each
 * <p/>
 * The algorithm will produce the ideal scheduling, with some jobs having no slaves, these jobs shall then be assigned
 * to the freest slaves producing the theoretically best possible global scheduling
 */
public class MaxFlowScheduler {

    class Node<E> {
        HashMap<Node<E>, Integer> neighbours;
        E id;

        public Node(E data) {
            this.id = data;
        }

        public void setNeighbour(Node<E> neighbour, int capacity) {
            neighbours.put(neighbour, capacity);
        }

        public Set<Node<E>> getNeighbours() {
            return neighbours.keySet();
        }
    }

    public List<Task> assignSlaveIdToTasks(List<Task> tasks, HashMap<Integer, List<Integer>> jobsToIdealSlaveMap, HashMap<Integer, Integer> slaveToFreeCoresMap) {
        Node<Integer> graph = buildGraph(jobsToIdealSlaveMap, slaveToFreeCoresMap);
        return tasks;
    }

    /**
     * Responsible for building the graph on which Ford Fulkerson shall be run
     * Creates graph according to specification described in the model
     *
     * @param jobsToIdealSlaveMap Map from jobId to a list of integers with slaveIds that are the best for that job
     *                            Likely because they contain the block which is the target of the map/reduce operation
     * @param slaveToFreeCoresMap Map from slaves to number of cores on slave minus the number of jobs currently being
     *                            run on that slave
     * @return
     */
    private Node<Integer> buildGraph(HashMap<Integer, List<Integer>> jobsToIdealSlaveMap, HashMap<Integer, Integer> slaveToFreeCoresMap) {
        //Produce job nodes
        List<Node<Integer>> jobs = new ArrayList<Node<Integer>>();
        for (Integer jobID : jobsToIdealSlaveMap.keySet()) {
            jobs.add(new Node<Integer>(jobID));
        }
        //Produce slave nodes
        HashMap<Integer, Node<Integer>> slaves = new HashMap<Integer, Node<Integer>>();
        for (Integer slaveId : slaveToFreeCoresMap.keySet()) {
            slaves.put(slaveId, new Node<Integer>(slaveId));
        }

        // Create source, with capacity one edges from source to each job
        Node<Integer> source = new Node<Integer>(-1);
        for (Node<Integer> job : jobs) {
            source.setNeighbour(job, 1);
        }

        //Create sink, with capacity free cores edge going from each slave to sink
        Node<Integer> sink = new Node<Integer>(-1);
        for (Node<Integer> slave : slaves.values()) {
            slave.setNeighbour(sink, slaveToFreeCoresMap.get(slave.id));
        }

        //Create edges between each job and it's ideal slave (which has the block)
        for (Node<Integer> job : jobs) {
            for (int slaveId : jobsToIdealSlaveMap.get(job.id)) {
                job.setNeighbour(slaves.get(slaveId), Integer.MAX_VALUE);
            }
        }

        return source;
    }
}
