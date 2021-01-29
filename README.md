# pandadb-v0.3
intelligent graph database

<img src="https://github.com/grapheco/pandadb-v0.3/blob/master/docs/logo.jpg?raw=true">

* intelligent property graph mgmt
* distributed non-Neo4j graph
* Bamboo as costore backend
* RegionFS as BLOB storage backend

# Licensing
PandaDB v0.3 is an open source product licensed under Apache2.0

#Limitation

## 1. Building PandaDB
### 1.1 install all artifacts
```
mvn clean install
```
###1.2 building server-side distribution zip package
```
cd packaging

mvn package -Pserver-unix-dist
```
this command will create `pandadb-server-<version>.jar` in `target` directory.


## 2. Quick start
#### 2.1 Download package

visit https://github.com/grapheco/pandadb-v0.3/releases to get pandadb-v0.3 binary distributions.

unpack `pandadb-server-<version>-unix.tar.gz` in your local directory, e.g. `/usr/local/`.

#### 2.2 Modify the configuration file (optional)
```
cd /usr/local/pandadb-server-<version>
vim conf/pandadb.conf
```
start pandadb with the default configuration will create database in the `usr/local/panda-server-<version>/data` directory.

#### 2.3 Start
```
cd /usr/local/pandadb-server-<version>

./bin/pandadb.sh [start || console]
```

## 3. Data import
use the shell script: `/usr/local/pandadb-server-<version>/bin/importer-panda.sh`  

**params:**
* `-db-path`    : path of database, need an empty folder.
* `--nodes`     : nodes csv files.
* `--relationships`     : relationships csv files.
* `--delimeter`     separator of csv file, default is `,`.
* `--array-delimeter` array's separator in your csv file, default is `|`. 

example: 
```
./importer-panda.sh --db-path=/pandadb --nodes=node1.csv --nodes=node2.csv --relationships=rels1.csv --delimeter="," --array-delimeter="|"
```

## 4. Cypher shell
script location: `/usr/local/pandadb-server-<version>/bin/cypher-shell`  

usage: `./cypher-shell -a panda://localhost:9989 -u pandadb -p pandadb`