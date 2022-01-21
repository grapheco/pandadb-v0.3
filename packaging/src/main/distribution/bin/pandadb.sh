#!/usr/bin/env bash

get_pandadb_home(){
  PANDADB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
  PANDADB_CONF=$PANDADB_HOME"/conf/pandadb.conf"
  PANDADB_LOG=$PANDADB_HOME"/logs/debug.log"
  PANDADB_DATA=$PANDADB_HOME"/data/"
  PANDADB_JAR=$PANDADB_HOME"/lib/panda-entry-point.jar"
  PANDADB_LAB=$PANDADB_HOME"/lib"
  PANDADB_ENTRY="org.grapheco.pandadb.server.DistributedPandaServerEntryPoint"
}
check_files() {
  if [ -e $PANDADB_CONF ]
  then :
  else
    echo "$PANDADB_CONF not exist"
    exit 1
  fi

  if [ ! -f $PANDADB_LOG ]
  then touch $PANDADB_LOG
  fi
}

do_console(){
   PANDADB_PID=`pgrep -f "DistributedPandaServerEntryPoint"`
    if [ "$PANDADB_PID" != "" ]
    then echo "pandadb is running on pid: $PANDADB_PID"
    else
      java -cp "$PANDADB_LAB""//*" $PANDADB_ENTRY $PANDADB_CONF
    fi
}
do_start(){
  PANDADB_PID=`pgrep -f "DistributedPandaServerEntryPoint"`
  if [ "$PANDADB_PID" != "" ]
  then echo "pandadb is running on pid: $PANDADB_PID"
  else
    nohup java -cp "$PANDADB_LAB""//*" $PANDADB_ENTRY $PANDADB_CONF > $PANDADB_LOG 2>&1 &
    cat $PANDADB_CONF | while read line
    do
      result1=$(echo $line | grep "rpc.listen.host")
      result2=$(echo $line | grep "rpc.listen.port")
      if [ "$result1" != "" ] || [ "$result2" != "" ]
      then echo $line
      fi
    done
    PANDADB_PID=`pgrep -f "DistributedPandaServerEntryPoint"`
    echo "pandadb server started...pid: $PANDADB_PID"
  fi
}
do_stop(){
  PANDADB_PID=`pgrep -f "DistributedPandaServerEntryPoint"`
  if [ "$PANDADB_PID" == "" ]
  then echo "pandadb not running..."
  else
    kill -15 $PANDADB_PID
    echo "pandadb server stopped...pid: $PANDADB_PID"
  fi
}
main(){
  get_pandadb_home
  check_files

  case "${1:-}" in
  console)
    do_console
    ;;

  start)
    do_start
    ;;

  stop)
    do_stop
    ;;

  help)
    echo "Usage: { console | start | stop }"
    ;;
  *)
    echo >&2 "Usage: { console | start | stop}"
    exit 1
    ;;
  esac
}

main "$@"