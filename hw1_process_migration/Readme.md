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
java -cp classes/ distsys.ProcessManager
```
And the client from a different terminal:
```
java -cp classes/ distsys.ProcessManager -c [hostname (e.g. localhost)]
```

To clean up the messy classes, call the following from the root directory:
```
make clean
```

when entering process name to launch a new process, use fully qualified name:
distsys.process.FileCountProcess 500 count_test.txt

To kill a process, use the full name obtained from the process list.

Always launch master first, then the slaves so that they can connect to the master.
The master will automatically load balance processes over the slaves.

