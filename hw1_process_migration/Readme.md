# Distributed Systems

### Lab 1 - Process Migration
### 15-640/440

* Kevin Crane
* Prashanth Balasubramaniam

To compile from root directory (the one with *Makefile*):
```
make
```
This must be done from each terminal that is running the application.


To run the host (master):
```
cd src
java -cp ../classes/ distsys.ProcessManager
```
And the client from a different terminal:
```
cd src
java -cp ../classes/ distsys.ProcessManager -c [hostname (e.g. localhost)]
```

To clean up the messy classes, call the following from the root directory:
```
make clean
```
