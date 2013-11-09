# Distributed Systems

### Lab 2 - RMI Facility
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
java -cp out/ distsys.RmiServer
```
And the client from a different terminal:
```
java -cp out/ distsys.RmiClientMaths server_hostname
java -cp out/ distsys.RmiClientSleep server_hostname [sleep_time]
```

To clean up the messy classes, call the following from the root directory:
```
make clean
```
