#!/bin/bash
#
# run script for MacOSX.  Run in the top level directory of project.
# Controller will start in the terminal, and a new window will be opened.
# This new window will spawn MULTI + 1 Chunk Servers.
#

MULTI="1 2 3 4 5 6 7 8 9"

DIR="$( cd "$( dirname "$0" )" && pwd )"
JAR_PATH="$DIR/conf/:$DIR/build/libs/fault-tolerant-file-system.jar"
COMPILE="$( ps -ef | grep [c]s555.system.node.Controller )"

SCRIPT="java -cp $JAR_PATH cs555.system.node.ChunkServer;"

function new_tab() {
    osascript \
        -e "tell application \"Terminal\"" \
        -e "tell application \"System Events\" to keystroke \"t\" using {command down}" \
        -e "do script \"$SCRIPT\" in front window" \
        -e "end tell" > /dev/null
}

if [ -z "$COMPILE" ]
then
LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
    echo Project has "$LINES" lines
    gradle clean
    gradle build
    open -a Terminal .
    open -a Terminal .
    java -cp $JAR_PATH cs555.system.node.Controller;
elif [ $1 = "c" ]
then
    java -cp $JAR_PATH cs555.system.node.Client;
else
    if [ -n "$MULTI" ]
    then
        for tab in `echo $MULTI`
        do
            new_tab
        done
    fi
    eval $SCRIPT
fi
