#!/bin/bash
PID=0
sigterm_handler() {
  echo "Hazelcast Term Handler received shutdown signal. Signaling hazelcast instance on PID: ${PID}"
  if [ ${PID} -ne 0 ]; then
    kill "${PID}"
  fi
}

PRG="$0"
PRGDIR=`dirname "$PRG"`
HAZELCAST_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`/hazelcast
PID_FILE=$HAZELCAST_HOME/hazelcast_instance.pid

if [ "x$MIN_HEAP_SIZE" != "x" ]; then
	JAVA_OPTS="$JAVA_OPTS -Xms${MIN_HEAP_SIZE}"
fi

if [ "x$MAX_HEAP_SIZE" != "x" ]; then
	JAVA_OPTS="$JAVA_OPTS -Xmx${MAX_HEAP_SIZE}"
fi

# if we receive SIGTERM (from docker stop) or SIGINT (ctrl+c if not running as daemon)
# trap the signal and delegate to sigterm_handler function, which will notify hazelcast instance process
trap sigterm_handler SIGTERM SIGINT

export CLASSPATH=$HAZELCAST_HOME/hazelcast-all-$HZ_VERSION.jar:$CLASSPATH/*

EC2_PRIVATE_ADDRESS=`curl -s http://169.254.169.254/latest/meta-data/local-ipv4`

echo "########################################"
echo "# RUN_JAVA=$RUN_JAVA"
echo "# JAVA_OPTS=$JAVA_OPTS"
echo "# starting now...."
echo "########################################"

java -server $JAVA_OPTS -Dec2_private_address=$EC2_PRIVATE_ADDRESS com.hazelcast.core.server.StartServer &
PID="$!"
echo "Process id ${PID} for hazelcast instance is written to location: " $PID_FILE
echo ${PID} > ${PID_FILE}

# wait on hazelcast instance process
wait ${PID}
# if a signal came up, remove previous traps on signals and wait again (noop if process stopped already)
trap - SIGTERM SIGINT
wait ${PID}