# Makefile
# 15-640/440 Project 4
# Kevin Crane
# Prashanth Balasubramaniam


default:
	find ./src -name *.java > sources_list.txt
	mkdir -p ./out
	javac -d ./out @sources_list.txt
	rm sources_list.txt

clean: 
	rm -r ./out

# Slaves:
#   java -cp out/ distsys.SlaveNode [port_number]
# Master:
#   java -cp out/ distsys.MapReduceManager
# See Readme.md for more info
