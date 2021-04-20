# pandadb-v0.3
intelligent graph database

<img src="docs/logo.jpg" width="128">

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
vim conf/rocksdb.conf
```
start pandadb with the default configuration will create database in the `usr/local/panda-server-<version>/data` directory.

#### 2.3 Start
```
cd /usr/local/pandadb-server-<version>

./bin/pandadb.sh start

```
notice: `./bin/pandadb.sh` will show help.

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
./importer-panda.sh --db-path=/pandadb 
--nodes=/testdata/node1.csv --nodes=testdata/node2.csv
--relationships=testdata/rels1.csv --relationships=testdata/rels2.csv
--delimeter="," --array-delimeter="|"
```
*NOTICE:*
1. node csv file
    - *MUST* specify append `:ID` in your major column of csv file and this column must be *NUMBER* type.
    - *OPTION* specify append `:LABEL` in your label column of csv file, default is `default`.  
2. relationship csv file
    - *MUST* contain `REL_ID` column in your relationship csv, if append `:IGNORE`, will not add this REL_ID as property.
    - *MUST* specify append `:TYPE` in your relation type column of csv file.
    - *MUST* specify append `:START_ID` and `:END_ID` for your columns of start node id and end node id
3. delimeter and array-delimeter cannot be the same
4. supported data types
    - int, long, boolean, double, string, date
    - string[], int[], long[], boolean[], double[]
    - BLOB
    - default: string

node csv example:  
| nodeId:ID | label:LABEL | name | jobs:string[] |  
| :----: | :----: | :----: | :----: |  
| 1 | person | alex | {teacher\|coder\|singer} |
  
relationship csv example:
| REL_ID:IGNORE | relation:TYPE | boss:START_ID | worker:END_ID |  
| :----: | :----: | :----: | :----: |  
| 1 | friend | 1 | 2 |

## 4. Cypher shell
script location: `/usr/local/pandadb-server-<version>/bin/cypher-shell`  

usage: `./cypher-shell -a panda://localhost:9989 -u "" -p ""`

quit: `:quit`
## 5. Driver
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

## 6. Embedding mode
usage:
```
// use default settings.
val service = GraphDatabaseBuilder.newEmbeddedDatabase("/home/pandadb")
// use setting file.
val service = GraphDatabaseBuilder.newEmbeddedDatabase("/home/pandadb", "/home/conf/rocksdb.conf")
	
service.addNode(Map("name"->"alex"), "person", "coder")
service.addNode(Map("name"->"joy","age"->22), "worker")
service.addRelation("friend", 1, 2, Map())

service.cypher(“match (n) return n”).show()
service.cypher("match (n)-[r]->(m) return r").show()
```
