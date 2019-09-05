#Use with caution!
killall -u $USER java

DIR="$( cd "$( dirname "$0" )" && pwd )"
MACHINE_LIST="$DIR/conf/machine_list"
APPLICATION_PROPERTIES="$DIR/conf/application.properties"

function prop {
    grep "${1}" ${APPLICATION_PROPERTIES}|cut -d'=' -f2
}

# Kill Chunk Server

for i in `cat $MACHINE_LIST`
do
	echo 'logging into '$i
	ssh $i "killall -u $USER java"
done

# Kill Controller

CONTROLLER=$(prop 'controller.host')
echo 'logging into '$CONTROLLER
ssh $CONTROLLER "killall -u $USER java"

# Kill Client

CONTROLLER=$(prop 'client.host')
echo 'logging into '$CONTROLLER
ssh $CONTROLLER "killall -u $USER java"
