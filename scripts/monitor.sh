#!/bin/bash
#
# tr -s '[:blank:]' ',' <test.txt 
#

DIR="$( cd "$( dirname "$0" )" && pwd )"
MACHINE_LIST="$DIR/conf/machine_list"
APPLICATION_PROPERTIES="$DIR/conf/application.properties"

function prop {
    grep "${1}" ${APPLICATION_PROPERTIES}|cut -d'=' -f2
}


if [[ $# -eq 0 ]] ; then
	echo './monitor Controller | Client | ChunkServer'
	exit 0
fi

INPUT=$1
PID=$(ps -ef | awk -v class="cs555.system.node.$INPUT$" '$NF~class {print $2}' | head -1)

if [ -z "$PID" ] ; then
	echo 'No process found'
	exit 0
fi

echo Monitoring the $INPUT with PID: $PID

top -b -d 0.01 -p ${PID} | grep --line-buffered ${PID} > $DIR/results/$HOSTNAME-$INPUT.txt
