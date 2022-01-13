# pandadb-v0.3
intelligent graph database

<img src="docs/logo.jpg" width="128">

# Feature
* intelligent property graph mgmt
* distributed non-Neo4j graph

# Licensing
???

## 1. Building PandaDB
### 1.1 install all artifacts
```
mvn clean install
```
### 1.2 building server-side distribution zip package
```
cd packaging

mvn package -Pserver-unix-dist
```
this command will create `pandadb-server-<version>.jar` in `target` directory.

## 2. Prepared
1. deploy ElasticSearch7.x or later version on your machines
2. deploy TiKV on your machines

## 3. start
#### 3.1 Download package

visit https://github.com/grapheco/pandadb-v0.3/releases to get pandadb-v0.3 binary distributions.

unpack `pandadb-server-<version>-unix.tar.gz` in your local directory, e.g. `/usr/local/`.

#### 3.2 Modify the configuration file
```
cd /usr/local/pandadb-server-<version>
vim conf/pandadb.conf
```
1. set ElasticSearch hosts address to `dbms.index.hosts`.
2. set TiKV PD hosts address to `dbms.kv.hosts`.
3. set PandaDB cluster nodes to `dbms.panda.nodes`
#### 3.3 Start
1. start your ElasticSearch service.
2. start your TiKV service. 
3. `./usr/local/pandadb-server-<version>/bin/pandadb.sh start`

notice: `./bin/pandadb.sh` will show help.

## 3. Data import
use the shell script: `/usr/local/pandadb-server-<version>/bin/importer-panda.sh`  

**params:**
* `--nodes`     : nodes csv files.
* `--relationships`     : relationships csv files.
* `--delimeter`     separator of csv file, default is `,`.
* `--array-delimeter` array's separator in your csv file, default is `|`. 
* `--kv-hosts`     : TiKV pd hosts.

example: 
```
./importer-panda.sh 
--nodes=/testdata/node1.csv 
--nodes=testdata/node2.csv
--relationships=testdata/rels1.csv 
--relationships=testdata/rels2.csv
--delimeter="," --array-delimeter="|"
--kv-hosts==127.0.0.1:2379,127.0.0.2:2379,127.0.0.3:2379
```
*NOTICE:*
1. node csv file
    - *MUST* contain `:ID` and `:LABEL` columns
2. relationship csv file
    - *MUST* contain `REL_ID`,`:TYPE`,`START_ID`,`END_ID` columns  
3. delimeter and array-delimeter cannot be the same
4. supported data types
    - int, long, boolean, double, string
    - string[], int[], long[], boolean[], double[]
    - default: string

node csv example:  
| nodeId:ID | label:LABEL | name | jobs:string[] |  
| :----: | :----: | :----: | :----: |  
| 1 | person | alex | [teacher\|coder\|singer] |
  
relationship csv example:
| REL_ID | relation:TYPE | :START_ID | :END_ID |  
| :----: | :----: | :----: | :----: |  
| 1 | friend | 1 | 2 |


## 4. Driver
visit https://github.com/grapheco/pandadb-v0.3/releases to get pandadb-driver-1.0-SNAPSHOT.jar.   
then add the jar to your project, this driver only support `session.run()`.  
usage example:
```
  val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("", ""))
  val session = driver.session()
  
  val res1 = session.run("create (n:people{name:'alex', company:'google'}) return n")
  println(res1.next().get("n").asMap())
  
  val res2 = session.run(“match (n:people) return n”)
  while (res2.hasNext){
    println(res.next().get("n").asNode().get("name"))
  }
  
  val res3 = session.run(“match (n)-[r]->(m) return r”)
  while (res3.hasNext){
    println(res.next().get("r").asRelationship())
  }
  
  session.close()
  driver.close()
```
## 5. Cypher-shell
script location: `/usr/local/pandadb-server-<version>/bin/cypher-shell`  

usage: 
1. `./cypher-shell -a panda://127.0.0.1:9989 -u "" -p ""`
2. `./cypher-shell -a panda://127.0.0.1:9989,127.0.0.2:9989,127.0.0.3:9989 -u "" -p ""`  

exit: `:quit`

## 6. Extra
###  TiKV deploy
* add a linux user first, eg: `useradd tikv`
* install tiup
```
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
```
* source tiup
```
1. cd /home/tikv
2. source .bash_profile
```
* update tiup
```
tiup update --self && tiup update cluster
```
* generate cluster config template
```
tiup cluster template > topology.yaml
```
* config template settings
```
global:
  user: "tikv"
  ssh_port: 22
  deploy_dir: "/tikv-deploy"
  data_dir: "/tikv-data"
server_configs: 
    tikv:
pd_servers:
  - host: 127.0.0.1
  - host: 127.0.0.2
  - host: 127.0.0.3
tikv_servers:
  - host: 127.0.0.4
  - host: 127.0.0.5
  - host: 127.0.0.6
monitoring_servers:
  - host: 127.0.0.7
grafana_servers:
  - host: 127.0.0.7
```
* check service status and auto optimize
```
tiup cluster check ./topology.yaml --user root -p
tiup cluster check ./topology.yaml --apply --user root -p
```
* generate TiKV cluster
```
tiup cluster deploy cluster-tmp v5.3.0 ./topology.yaml --user root -p
```
cluster name: cluster-tmp  
tikv version: v5.3.0, check tikv version info: `tiup list tikv`
* check tikv cluster status and start 
```
tiup cluster display cluster-tmp
tiup cluster start cluster-tmp
```
