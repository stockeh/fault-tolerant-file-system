#!/bin/bash
#
# tr -s '[:blank:]' ',' <test.txt 
#
if [[ $# -eq 0 ]] ; then
	echo './monitor Controller | Client | ChunkServer'
	exit 0
fi

INPUT=$1
PID=$(ps | awk -v class="cs555.system.node.$INPUT$" '$NF~class {print $1}' | head -1)

if [ -z "$PID" ] ; then
	echo 'No process found'
	exit 0
fi

echo Monitoring the $INPUT with PID: $PID

top -l 0 -pid ${PID} | grep --line-buffered ${PID} > test.txt
