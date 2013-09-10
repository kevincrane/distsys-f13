# Makefile
# 15-640/440 Project 1
# Kevin Crane
# Prashanth Balasubramaniam

default:
	find ./src -name *.java > sources_list.txt
	mkdir -p ./classes 
	javac -d ./classes @sources_list.txt

clean: 
	rm -r ./classes
