# Makefile
# 15-640/440 Project 4
# Kevin Crane
# Prashanth Balasubramaniam


default:
	find ./src -name *.java > sources_list.txt
	mkdir -p ./out
	javac -d ./out -cp mpi.jar @sources_list.txt
	rm sources_list.txt

clean: 
	rm -r ./out

# Run Sequentially:
#   java -cp out:mpi.jar distsys.ClusterMainSeq {points|dna} [data_count] [cluster_count] (strand_length)
# Run with OpenMPI:
#   mpirun -np [num_hosts] -H localhost java -cp out:mpi.jar distsys.ClusterMainMPI {points|dna} [data_count] [cluster_count] (strand_length)
#   mpirun -np [num_hosts] -hostfile [host_file] java -cp out:mpi.jar distsys.ClusterMainMPI {points|dna} [data_count] [cluster_count] (strand_length)