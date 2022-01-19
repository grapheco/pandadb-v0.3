#!/bin/bash

NODESCOUNT=0
RELSCOUNT=0
DELIMETER="--delimeter=,"
ARRAY_DELIMETER="--array-delimeter=|"

get_kv_host(){
CONF_File=${PANDADB_HOME}/conf/pandadb.conf
for line in `cat ${CONF_File}`
do
  if [[ "$line" == dbms.kv.hosts* ]]
        then KV_HOSTS="--kv-hosts="${line#*=}
  fi
done
}

get_file_nodes_names(){
for file_name in ${PANDADB_DEMO_NODES}/*
do
  tmpFile="--nodes="${PANDADB_HOME}/demo/nodes/`basename $file_name`
  NODES[$NODESCOUNT]=$tmpFile
  let NODESCOUNT++
done
}
get_file_rels_names(){
for file_name2 in ${PANDADB_DEMO_RELS}/*
do
  tmpFile="--relationships="${PANDADB_HOME}/demo/relations/`basename $file_name2`
  RELS[$RELSCOUNT]=$tmpFile
  let RELSCOUNT++
done
}

main(){
PANDADB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
PANDADB_LAB=$PANDADB_HOME"/lib"
PANDADB_DEMO_NODES=$PANDADB_HOME"/demo/nodes"
PANDADB_DEMO_RELS=$PANDADB_HOME"/demo/relations"

get_kv_host
get_file_nodes_names
get_file_rels_names

java -cp "$PANDADB_LAB""//*" "org.grapheco.pandadb.tools.importer.PandaImporter" "${NODES[@]}" "${RELS[@]}" "--delimeter=|" "--array-delimeter=," "$KV_HOSTS"

}

main "$@"

