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

# Main Method:
#   java -cp out/ distsys.ClusterMain
