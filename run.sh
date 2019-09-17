#!/bin/bash

#########################################################################
#                                                                       #
#    Fault Tolerant File System Using Replication and Erasure Coding    #
#                                                                       #
#             Jason D Stock - stock - September 04, 2019                #
#                                                                       #
#########################################################################

# Configurations

DIR="$( cd "$( dirname "$0" )" && pwd )"
JAR_PATH="$DIR/conf/:$DIR/build/libs/fault-tolerant-file-system.jar"
MACHINE_LIST="$DIR/conf/machine_list"
APPLICATION_PROPERTIES="$DIR/conf/application.properties"

function prop {
    grep "${1}" ${APPLICATION_PROPERTIES}|cut -d'=' -f2
}

# Launch Controller

LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
echo Project has "$LINES" lines

gradle clean; gradle build
gnome-terminal --geometry=170x60 -e "ssh -t $(prop 'controller.host') 'java -cp $JAR_PATH cs555.system.node.Controller; bash;'"

sleep 1

# Launch Chunk Servers

SCRIPT="java -cp $JAR_PATH cs555.system.node.ChunkServer"

for ((j=0; j<${1:-1}; j++))
do
    COMMAND='gnome-terminal'
    for i in `cat $MACHINE_LIST`
    do
        echo 'logging into '$i
        OPTION='--tab -e "ssh -t '$i' '$SCRIPT'"'
        COMMAND+=" $OPTION"
    done
    eval $COMMAND &
done

sleep 4

# Launch Client

gnome-terminal --geometry=132x60 -e "ssh -t $(prop 'client.host') 'java -cp $JAR_PATH cs555.system.node.Client; bash;'"
