# pandadb-v0.3
intelligent graph database

<img src="docs/logo.jpg" width="128">

# Feature
* intelligent property graph mgmt
* distributed non-Neo4j graph

# License
PandaDB is under the Apache 2.0 license.

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
3. start pandadb server on each machine. `${pandadb-home}/bin/pandadb.sh start`

notice: `${pandadb-home}/bin/pandadb.sh` will show help.

## 3. Data import
use the shell script: `${pandadb-home}/bin/importer-panda.sh`  

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
- - -
### LDBC Demo data import
```
cd ${pandadb-home}

./bin/demo-importer.sh --kv-hosts=${YOUR-TIKV-PD-ADDRESS}
```
eg: `./${pandadb-home}/bin/demo-importer.sh --kv-hosts=127.0.0.1:2379,127.0.0.2:2379,127.0.0.3:2379`


## 4. Driver
pandadb-java-driver can easily switch to neo4j driver or pandadb driver, all the user needs to do is change the connection schema to `bolt` or `panda`. when connected to pandadb, only support `session.run()` method to run cypher.  

visit https://github.com/grapheco/pandadb-v0.3/releases to get pandadb-java-driver.jar.   

usage example:
```
 import org.neo4j.driver.v1.*;
 import org.neo4j.driver.v1.types.Node;
 import org.neo4j.driver.v1.types.Relationship;
 
 public class DriverDemo {
     public static void main(String[] args) {
         Driver driver = GraphDatabase.driver("panda://127.0.0.1:9989", AuthTokens.basic("", ""));
         // Driver driver = GraphDatabase.driver("panda://127.0.0.1:9989,127.0.0.2:9989,127.0.0.3:9989", AuthTokens.basic("", ""));
 
         Session session = driver.session();
         StatementResult result1 = session.run("match (n) return n limit 10");
         while (result1.hasNext()){
             Record next = result1.next();
             Node n = next.get("n").asNode();
             System.out.print("node labels: ");
             n.labels().iterator().forEachRemaining(System.out::println);
             System.out.println("node properties: " + n.asMap());
             System.out.println();
         }
 
         StatementResult result2 = session.run("match (n)-[r]->(m) return r limit 10");
         while (result2.hasNext()){
             Record next = result2.next();
             Relationship r = next.get("r").asRelationship();
             System.out.println("rel type: " + r.type());
             System.out.println("rel start node id: " + r.startNodeId());
             System.out.println("rel end node id: " + r.endNodeId());
             System.out.println("rel properties: " + r.asMap());
             System.out.println();
         }
 
         session.close();
         driver.close();
     }
 }

```
## 5. Extra
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
* destroy tikv cluster
```
tiup cluster destroy cluster-tmp
```
