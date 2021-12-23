#!/bin/bash

NODESCOUNT=0
RELSCOUNT=0
DELIMETER="--delimeter=,"
ARRAY_DELIMETER="--array-delimeter=|"

help_func(){
  echo ""
  echo "====================importer need parameters are as follows===================="
  echo "[--nodes]:            your nodes csv files"
  echo "[--relationships]:    your relationship files"
  echo "[--delimeter]:        your separator of csv file, default is ',' "
  echo "[--array-delimeter]:  array's separator in your csv file, default is '|' "
  echo "[--kv-hosts]:         your tikv hosts"

  echo ""
  echo "example: [./importer-panda.sh  --nodes=n1.csv --nodes=n2.csv --kv-hosts='127.0.0.1:2379,127.0.0.2:2379,127.0.0.3:2379'  --delimeter=\| --array-delimeter=,]  "
  echo "================================================================================"
  echo ""
}

get_params_func(){
for arg in $@
do
  if [[ $arg == "--nodes"* ]]
  then
    NODES[$NODESCOUNT]=$arg
        let NODESCOUNT++
  elif [[ $arg == "--relationships"* ]]
  then
    RELS[$RELSCOUNT]=$arg
        let RELSCOUNT++
  elif [[ $arg == "--delimeter"* ]]
  then
    DELIMETER=$arg
  elif [[ $arg == "--array-delimeter"* ]]
  then
    ARRAY_DELIMETER=$arg
  elif [[ $arg == "--kv-hosts"* ]]
  then
    KV_HOSTS=$arg
  fi
done
}

main(){
PANDADB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
PANDADB_LAB=$PANDADB_HOME"/lib"

if [[ $1 = "--help" ]] || [[ $1 = "-h" ]] || [[ $# = 0 ]]
then
  help_func
  exit 1
fi
get_params_func "$@"

java -cp "$PANDADB_LAB""//*" "cn.pandadb.tools.importer.PandaImporter" "${NODES[@]}" "${RELS[@]}" "$DELIMETER" "$ARRAY_DELIMETER" "$KV_HOSTS"

}

main "$@"