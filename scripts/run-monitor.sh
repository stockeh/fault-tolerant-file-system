#!/bin/bash

DIR="$( cd "$( dirname "$0" )" && pwd )"
MACHINE_LIST="$DIR/../conf/machine_list"
APPLICATION_PROPERTIES="$DIR/../conf/application.properties"

function prop {
    grep "${1}" ${APPLICATION_PROPERTIES}|cut -d'=' -f2
}

gnome-terminal --geometry=170x60 -e "ssh -t $(prop 'controller.host') 'cd $DIR; ./monitor.sh Controller; bash;'"

sleep 1

# Launch Chunk Servers

SCRIPT="cd $DIR; ./monitor.sh ChunkServer"

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

sleep 1

# Launch Client

gnome-terminal --geometry=132x60 -e "ssh -t $(prop 'client.host') 'cd $DIR; ./monitor.sh Client; bash;'"
