#!/bin/bash

# Should be run from unix1.andrew.cmu.edu (or wherever is listed in Config.MASTER_NODE)
USER="kevincra"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
declare -a arr=(2 3 5 7 11)     # Host numbers of the slaves (e.g. unix2, unix3, ...)

make

# Run SlaveNode in multiple places
for host in ${arr[@]}
do
    ssh $USER@unix$host.andrew.cmu.edu "mkdir -p $DIR/slave$host; cd $DIR/slave$host; java -cp ../out/ distsys.SlaveNode &"
done

#Run the Master
java -cp ../out/ distsys.MapReduceManager
