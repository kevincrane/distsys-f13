# Makefile
# 15-640/440 Project 2
# Kevin Crane
# Prashanth Balasubramaniam

#TODO: remake this for HW2

default:
	find ./src -name *.java > sources_list.txt
	mkdir -p ./classes 
	javac -d ./classes @sources_list.txt
	rm sources_list.txt

clean: 
	rm -r ./classes
