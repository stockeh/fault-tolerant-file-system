HOST=Jasons-MacBook-Pro.local
PORT=5001

DIR="$( cd "$( dirname "$0" )" && pwd )"
BUILD="$DIR/build/classes/java/main"
COMPILE="$( ps -ef | grep [c]s555.system.node.Test )"

if [ -z "$COMPILE" ]
then
LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
    echo Project has "$LINES" lines
    gradle clean; gradle build
    pushd $BUILD; java -cp . cs555.system.node.Test $PORT; popd;
fi