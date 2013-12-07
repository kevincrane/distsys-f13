# Distributed Systems

### Lab 4 - K-Means Clustering (sequential & parallel w/ OpenMPI)
### 15-640/440

* Kevin Crane
* Prashanth Balasubramaniam


NOTE: In order to get Java bindings for OpenMPI, you need to download a Nightly version of OpenMPI from http://www.open-mpi.org/nightly/trunk/index.php, build it:
```
./configure --enable-mpi-java
make
make install
```
and hunt for mpi.jar (./ompi/mpi/java/java/mpi.jar). I put the file in this repo to make life easier, so hopefully nothing ever breaks.


To compile from root directory (the one with *Makefile*):
```
make
```
This must be done from each terminal that is running the application.

There are two data types supported with this program, 2D points and DNA strands. You must select which you want through a command-line argument, along with the number of points, clusters, and (if applicable) length of DNA strand. Running this algorithm in sequential fashion is easy. Just run:
```
java -cp out:mpi.jar distsys.ClusterMainSeq {points|dna} [data_count] [cluster_count] (strand_length)
```
For example:
```
java -cp out:mpi.jar distsys.ClusterMainSeq points 1000 5
java -cp out:mpi.jar distsys.ClusterMainSeq dna 500 4 30
```
To run in parallel across multiple hosts (or locally with multiple processors), call:
```
mpirun -np [num_hosts] -H [host_name] java -cp out:mpi.jar distsys.ClusterMainMPI {points|dna} [data_count] [cluster_count] (strand_length)
mpirun -np [num_hosts] -hostfile [host_file] java -cp out:mpi.jar distsys.ClusterMainMPI {points|dna} [data_count] [cluster_count] (strand_length)
```
where the hostfile is just a text file with the names of each available host in the MPI cluster on its own line. For example:
```
mpirun -np 4 -H localhost java -cp out:mpi.jar distsys.ClusterMainMPI points 1000 5
mpirun -np 12 -hostfile host_file java -cp out:mpi.jar distsys.ClusterMainMPI dna 500 4 30
```

To clean up all of these messy classfiles later, you can type:
```
make clean
```

That's it!
