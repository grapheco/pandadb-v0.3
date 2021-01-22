#!/usr/bin/env bash

get_pandadb_home(){
  PANDADB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
  PANDADB_CONF=$PANDADB_HOME"/conf/pandadb.conf"
  PANDADB_LOG=$PANDADB_HOME"/logs/debug.log"
  PANDADB_JAR=$PANDADB_HOME"/lib/pandadb-server-all-in-one-0.3.jar"
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
  java -jar $PANDADB_JAR $PANDADB_CONF
}
do_start(){
  nohup java -jar $PANDADB_JAR $PANDADB_CONF > $PANDADB_LOG 2>&1 &
  PANDADB_PID=`pgrep -f "pandadb-server"`
  echo "pandadb server started...pid: $PANDADB_PID"
}
do_stop(){
  PANDADB_PID=`pgrep -f "pandadb-server"`
  kill -15 $PANDADB_PID
  echo "pandadb server stopped...pid: $PANDADB_PID"
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
    echo "Usage: { console | start | stop | status | version }"
    ;;
  *)
    echo >&2 "Usage: { console | start | stop | status | version }"
    exit 1
    ;;
  esac
}

main "$@"