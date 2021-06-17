#!/bin/bash

NODESCOUNT=0
RELSCOUNT=0
DELIMETER="--delimeter=,"
ARRAY_DELIMETER="--array-delimeter=|"

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
  elif [[ $arg == "--nodeDBPath"* ]]
  then
    NODEDB_PATH=$arg
  elif [[ $arg == "--relationDBPath"* ]]
  then
    RELATIONDB_PATH=$arg
  elif [[ $arg == "--nodeLabelDBPath"* ]]
  then
    NODELABELDB_PATH=$arg
  elif [[ $arg == "--inRelationDBPath"* ]]
  then
    INRELATIONDB_PATH=$arg
  elif [[ $arg == "--outRelationDBPath"* ]]
  then
    OUTRELATIONDB_PATH=$arg
  elif [[ $arg == "--relationTypeDBPath"* ]]
  then
    RELATIONTYPEDB_PATH=$arg
  elif [[ $arg == "--rocksConf"* ]]
  then
    ROCKS_CONF=$arg
  elif [[ $arg == "--advanced-mode"* ]]
  then
    ADVANCED_MODE=$arg
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

java -cp "$PANDADB_LAB""//*" "cn.pandadb.tools.importer.PandaImporter" "$DBPATH" "${NODES[@]}" "${RELS[@]}" "$DELIMETER" "$ARRAY_DELIMETER" "$NODEDB_PATH" "$NODELABELDB_PATH" "$RELATIONDB_PATH" "$INRELATIONDB_PATH" "$OUTRELATIONDB_PATH" "$RELATIONTYPEDB_PATH" "$ROCKS_CONF" "$ADVANCED_MODE"

}

main "$@"