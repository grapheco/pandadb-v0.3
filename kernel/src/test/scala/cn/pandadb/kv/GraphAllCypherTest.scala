package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxNode, LynxResult, LynxValue}
import org.junit.{After, Assert, Before, Test}

class GraphAllCypherTest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacadeWithPPD = _

  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata"))
    new File("./testdata/output").mkdirs()

    val dbPath = "./testdata"
    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath+"/statistics")


    graphFacade = new GraphFacadeWithPPD(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }

  @Test
  def test1(): Unit ={
    val cy = "CREATE (n),(m) return n,m"
    val res = graphFacade.cypher(cy).show()
    Assert.assertEquals(2, nodeStore.allNodes().size)

  }

  @Test
  def test2(): Unit ={
    val cy = "CREATE (friend:Person {name: 'Mark'}) RETURN friend"
    val res = graphFacade.cypher(cy)
    Assert.assertEquals(List("Person"), res.records.toSeq.apply(0).apply("friend").asInstanceOf[LynxNode].labels)
    Assert.assertEquals(1, nodeStore.allNodes().size)
    //Assert.assertEquals(1, nodeStore.allNodes().size)

  }

  @Test
  def test3(): Unit ={
    val cy1 = "create (jennifer:Person {name: 'Jennifer'}), (mark:Person {name: 'Mark'}) return jennifer, mark"
    graphFacade.cypher(cy1).show()
    val cy = "MATCH (jennifer:Person {name: 'Jennifer'}),(mark:Person {name: 'Mark'}) CREATE (jennifer)-[rel:IS_FRIENDS_WITH]->(mark)"
    graphFacade.cypher(cy).show()

    Assert.assertEquals(2, nodeStore.allNodes().size)

    Assert.assertEquals(1, relationStore.allRelations().size)

  }

  @Test
  def test4(): Unit ={
    val cy1 = "CREATE (p:Person)-[:LIKES]->(t:Technology) return p,t"
    graphFacade.cypher(cy1).show()

    Assert.assertEquals(2, nodeStore.allNodes().size)

    Assert.assertEquals(1, relationStore.allRelations().size)

  }


  @Test
  def test5(): Unit ={
    val cy1 = "CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE"
    graphFacade.cypher(cy1).show()

    Assert.assertEquals(2, nodeStore.allNodes().size)

    Assert.assertEquals(1, relationStore.allRelations().size)

  }

  @Test
  def test6(): Unit ={
    val cy = "create (a:Person {name: 'A'}), (b:Person {name: 'B'}) return a, b"
    graphFacade.cypher(cy).show()

    val cy1 = "MATCH (a:Person),(b:Person) WHERE a.name = 'A' AND b.name = 'B' CREATE (a)-[r:RELTYPE { name: a.name + '<->' + b.name }]->(b) RETURN type(r), r.name"
    graphFacade.cypher(cy1).show()

    Assert.assertEquals(2, nodeStore.allNodes().size)

    Assert.assertEquals(1, relationStore.allRelations().size)

  }


  @Test
  def test7(): Unit ={
    val cy = "CREATE p =(andy { name:'Andy' })-[:WORKS_AT]->(neo)<-[:WORKS_AT]-(michael { name: 'Michael' }) RETURN p"
    graphFacade.cypher(cy).show()


    Assert.assertEquals(3, nodeStore.allNodes().size)

    Assert.assertEquals(2, relationStore.allRelations().size)

  }


  @Test
  def test8(): Unit ={
    val cy = "CREATE (n:t2) return n"
    graphFacade.cypher(cy).show()

    val cy1 = "match (n:t2) return n"
    graphFacade.cypher(cy1).show()

    Assert.assertEquals(1, nodeStore.allNodes().size)

    //Assert.assertEquals(2, relationStore.allRelations().size)

  }


  @Test
  def test9(): Unit ={
    val cy = "CREATE (n:t2:t1) return n"
    graphFacade.cypher(cy).show()

    val cy1 = "match (n:t1:t2) return n"
    graphFacade.cypher(cy1).show()

    Assert.assertEquals(1, nodeStore.allNodes().size)
    //Assert.assertEquals(2, relationStore.allRelations().size)
  }


  @Test
  def test10(): Unit ={
    val cy = "CREATE (n{name:'bluejoe'}) return n"
    graphFacade.cypher(cy).show()

    val cy1 = "match (n) where n.name='bluejoe' return n"
    graphFacade.cypher(cy1).show()

    Assert.assertEquals(1, nodeStore.allNodes().size)
    //Assert.assertEquals(2, relationStore.allRelations().size)
  }


  @Test
  def test11(): Unit ={
    val cy = "CREATE (m)-[r:type]->(n) return m"
    graphFacade.cypher(cy).show()

    val cy1 = "match (m)-[r]->(n) return m,r,n"
    graphFacade.cypher(cy1).show()

    Assert.assertEquals(2, nodeStore.allNodes().size)
    Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test12(): Unit ={
    val cy = "CREATE (m)-[r:type]->(n) return m"
    graphFacade.cypher(cy).show()

    val cy1 = "match (n)-[r]-(m) return distinct r"
    graphFacade.cypher(cy1).show()

    Assert.assertEquals(2, nodeStore.allNodes().size)
    Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test13(): Unit ={
    //val cy = "CREATE (m)-[r:type]->(n) return m"
   // graphFacade.cypher(cy).show()

    val cy1 = "return 1 as N"
    val rs = graphFacade.cypher(cy1)
    Assert.assertEquals(Seq("N"), rs.columns)
    //Assert.assertEquals(2, nodeStore.allNodes().size)
    //Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test14(): Unit ={
    //val cy = "CREATE (m)-[r:type]->(n) return m"
    // graphFacade.cypher(cy).show()

    val cy1 = "return 1 "
    val rs = graphFacade.cypher(cy1)
    Assert.assertEquals(Seq("1"), rs.columns)
    //Assert.assertEquals(2, nodeStore.allNodes().size)
    //Assert.assertEquals(1, relationStore.allRelations().size)
  }


  @Test
  def test15(): Unit ={
    val cy = "CREATE (m:Person{name:'Jennifer'}) return m"
    graphFacade.cypher(cy).show()

    val cy1 = "MATCH (jenn:Person {name: 'Jennifer'}) RETURN jenn"
    val rs = graphFacade.cypher(cy1).show()
    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(1, nodeStore.allNodes().size)
    //Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test16(): Unit ={
    val cy = "CREATE (n:Person {name: 'Jennifer'})-[:WORKS_FOR]->(company:Company{name:'haha'}) return n"
    graphFacade.cypher(cy).show()

    val cy1 = "MATCH (:Person {name: 'Jennifer'})-[:WORKS_FOR]->(company:Company) RETURN company.name"
    val rs = graphFacade.cypher(cy1).show()
    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(2, nodeStore.allNodes().size)
    Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test17(): Unit ={
    val cy = "CREATE (n:Person {name: 'Jennifer'})-[:WORKS_FOR]->(company:Company{name:'haha'}) return n"
    graphFacade.cypher(cy).show()

    val cy1 = "MATCH (:Person {name: 'Jennifer'})-[:WORKS_FOR]->(company:Company) RETURN company.name"
    val rs = graphFacade.cypher(cy1).show()
    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(2, nodeStore.allNodes().size)
    Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test18_update(): Unit ={
    val cy = "create (p:Person {name: 'Jennifer', birthdata:date('1980-01-01')})  RETURN p"
    graphFacade.cypher(cy).show()

    val cy1 = "MATCH (p:Person {name: 'Jennifer'}) SET p.birthdate = date('1980-01-01') RETURN p"
    val rs = graphFacade.cypher(cy1).show()
    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(1, nodeStore.allNodes().size)
   // Assert.assertEquals(1, relationStore.allRelations().size)
  }


  @Test
  def test19_delete(): Unit ={
    val cy = "create (p:Person {name: 'Jennifer'}) RETURN p"
    graphFacade.cypher(cy).show()

    val cy1 = "MATCH (p:Person {name: 'Jennifer'}) delete p"
    val rs = graphFacade.cypher(cy1).show()
    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(0, nodeStore.allNodes().size)
    // Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test20_remove(): Unit ={
    val cy = "create (p:Person {name: 'Jennifer'}) RETURN p"
    graphFacade.cypher(cy).show()

    val cy1 = "MATCH (n:Person {name: 'Jennifer'}) REMOVE n.name"
    val rs = graphFacade.cypher(cy1).show()
    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(0, nodeStore.allNodes().size)
    // Assert.assertEquals(1, relationStore.allRelations().size)
  }


  @Test
  def test21_merge(): Unit ={
    //val cy = "create (p:Person {name: 'Jennifer'}) RETURN p"
    // graphFacade.cypher(cy).show()

    val cy1 = "MATCH (j:Person {name: 'Jennifer'}),(m:Person {name: 'Mark'}) MERGE (j)-[r:IS_FRIENDS_WITH]->(m) RETURN j, r, m"
    val rs = graphFacade.cypher(cy1).show()
    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(2, nodeStore.allNodes().size)
    Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test22_Aggr(): Unit ={
    val cy = "create (a:Person {name: 'Jennifer', age:20}),(b:Person {name: 'Jennifer', age:12}), (c:Person {name: 'Jennifer', age:23})  RETURN a"
     graphFacade.cypher(cy).show()

    val cy1 = "match (n) return count(n)"
    val rs = graphFacade.cypher(cy1).show()

    val cy2 = "match (n) return Max(n.age)"
    val rs2 = graphFacade.cypher(cy2).show()

    val cy3 = "match (n) return sum(n.age)"
    val rs3 = graphFacade.cypher(cy3).show()
    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(3, nodeStore.allNodes().size)
    //Assert.assertEquals(1, relationStore.allRelations().size)
  }


  @Test
  def test23_param(): Unit ={
    val cy = "{'props' : [ {'name' : 'Andy','position' : 'Developer'}, {'name' : 'Michael','position' : 'Developer'} ]} UNWIND $props AS map CREATE (n) SET n = map"
    graphFacade.cypher(cy).show()


    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(2, nodeStore.allNodes().size)
    //Assert.assertEquals(1, relationStore.allRelations().size)
  }


  @Test
  def test24_foreach(): Unit ={
    val cy1 = "create (a:Person {name: 'Jennifer', age:20}),(b:Person {name: 'Jennifer', age:12}), (c:Person {name: 'Jennifer', age:23})  RETURN a"
    graphFacade.cypher(cy1).show()


    val cy = "MATCH p =(begin:person) FOREACH (n IN nodes(p)| SET n.marked = TRUE )"
    graphFacade.cypher(cy).show()


    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(3, nodeStore.allNodes().size)
    //Assert.assertEquals(1, relationStore.allRelations().size)
  }

  @Test
  def test25_foreach(): Unit ={
    val cy1 = "create (a:Person {name: 'Jennifer', age:20}),(b:Person {name: 'Jennifer', age:12}), (c:Person {name: 'Jennifer', age:23})  RETURN a"
    graphFacade.cypher(cy1).show()


    val cy = "MATCH (a:Person) return orderBy a.age"
    graphFacade.cypher(cy).show()


    //Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(3, nodeStore.allNodes().size)
    //Assert.assertEquals(1, relationStore.allRelations().size)
  }

}
