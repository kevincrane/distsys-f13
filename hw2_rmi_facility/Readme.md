# Distributed Systems

### Lab 2 - RMI Facility
### 15-640/440

* Kevin Crane
* Prashanth Balasubramaniam

TODO: REMAKE THIS FOR HOMEWORK 2




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

Use the fully qualified name, when entering the process name to launch a new process:
distsys.process.CountProcess 500
distsys.process.FileCountProcess 200 count_test.txt

To kill a process, use the full name obtained from the process list command.
Always launch master first, then the slaves so that the slaves can connect to the master. The master will automatically load balance processes over the slaves.


