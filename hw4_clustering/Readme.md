# Distributed Systems

TODO: update for project 4

### Lab 3 - Map Reduce Facility
### 15-640/440

* Kevin Crane
* Prashanth Balasubramaniam


To compile from root directory (the one with *Makefile*):
```
make
```
This must be done from each terminal that is running the application.


First you must run a SlaveNode on any number of terminals. Check with Config.java under SLAVE_NODES to see where the system will be looking to communicate. By default they point to unix#.andrew.cmu.edu (where we ran them while at CMU), but you should likely change these to localhost (e.g. ["localhost", "12345"]. Likewise, update MASTER_NODE in this file as needed (again, to "localhost" if that is where you will be running these). Once these are set to your liking, you will run one SlaveNode per terminal, each in their own folder (e.g. one in dir slave1, slave2, etc.). Call
```
java -cp out/ distsys.SlaveNode [port_number]
```
to run them.

Next, run the master in a similar way with
```
java -cp out/ distsys.MapReduceManager
```

When running it locally, run each slave in their own directory (e.g. slaves/slave1, slaves/slave2, etc.)

To clean up all of these messy classfiles later, you can type:
```
make clean
```

That's it!
