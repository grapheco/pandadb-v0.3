#!/bin/bash

NODESCOUNT=0
RELSCOUNT=0

add_node_file(){
  NODES[${NODESCOUNT}]=$1
  NODESCOUNT+=1
}
add_rels_file(){
  RELS[${RELSCOUNT}]=$1
  RELSCOUNT+=1
}

help_func(){
  echo ""
  echo "====================importer need parameters are as follows===================="
  echo "[--db-path]:           db path, need an empty folder"
  echo "[--nodes]:            your nodes csv files"
  echo "[--relationships]:    your relationship files"
  echo "[--delimeter]:        your separator of csv file, default is ',' "
  echo "[--array-delimeter]:  array's separator in your csv file, default is '|' "
  echo ""
  echo "example: [./importer-panda.sh --db-path=/pandadb/db --nodes=n1.csv --nodes=n2.csv --delimeter=\"|\" --array-delimeter=\",\"]  "
  echo "================================================================================"
  echo ""
}

get_params_func(){
for arg in $@
do
  if [[ $arg == "--db-path"* ]]
  then
    DBPATH=$arg
  elif [[ $arg == "--nodes"* ]]
  then
    NODES[$NODESCOUNT]=$arg
        NODESCOUNT+=1
  elif [[ $arg == "--relationships"* ]]
  then
    RELS[$RELSCOUNT]=$arg
        RELSCOUNT+=1
  elif [[ $arg == "--delimeter"* ]]
  then
    DELIMETER=$arg
  elif [[ $arg == "--array-delimeter" ]]
  then
    ARRAY_DELIMETER=$arg
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

java -cp "$PANDADB_LAB""//*" "cn.pandadb.tools.importer.PandaImporter" "$DBPATH" "${NODES[@]}" "${RELS[@]}" "$DELIMETER" "$ARRAY_DELIMETER"

}

main "$@"