package cn.pandadb.kv

import java.io.File
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.{GraphFacade, KeyConverter}
import cn.pandadb.kernel.kv.index.{IndexEncoder, IndexStoreAPI}
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.optimizer.AnyValue
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxNode, LynxResult}
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

class GraphFacadeTest {

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata"))
    new File("./testdata/output").mkdirs()
    val dbPath = "./testdata"

    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath+"/statistics")

    graphFacade = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  @After
  def close(): Unit = {
    graphFacade.close()
  }

  @Test
  def testCypherCreate(): Unit ={
    graphFacade.cypher("create (n{name:'123', age:12.2}) return n").show()
  }

  @Test
  def testAddDelete(): Unit = {
    Assert.assertEquals(0, nodeStore.allNodes().size)
    Assert.assertEquals(0, relationStore.allRelations().size)

    val nodeId1 = graphFacade.addNode(Map("name" -> "1"))
    val nodeId2 = graphFacade.addNode(Map("name" -> "2"))

    val relId1 = graphFacade.addRelation("1->2", nodeId1, nodeId2, Map())

    //nodes: {1,2}
    //rels: {1}
    Assert.assertEquals(List(1, 2), nodeStore.allNodes().toSeq.map(_.id).sorted)
    val relation = graphFacade.relationAt(relId1).get
    Assert.assertTrue(relation.startId.equals(nodeId1)&&relation.endId.equals(nodeId2))

    val nodeId3 = graphFacade.addNode(Map("name" -> "3"))
    //nodes: {1,2,3}
    Assert.assertEquals(List(nodeId1, nodeId2, nodeId3).sorted, nodeStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.deleteNode(nodeId2)
    //nodes: {1,3}
    Assert.assertEquals(List(nodeId1, nodeId3).sorted, nodeStore.allNodes().toSeq.map(_.id).sorted)

    val nodeId4 = graphFacade.addNode(Map("name" -> "4"))
    graphFacade.deleteNode(nodeId1)
    //nodes: {3,4}
    Assert.assertEquals(List(nodeId3, nodeId4).sorted, nodeStore.allNodes().toSeq.map(_.id).sorted)

  }

  @Test
  def testQuery(): Unit = {
    val n1: Long = graphFacade.addNode(Map("name" -> "bob", "age" -> 40), "person")
    val n2: Long = graphFacade.addNode(Map("name" -> "alex", "age" -> 20), "person")
    val n3: Long = graphFacade.addNode(Map("name" -> "simba", "age" -> 10), "worker")
    val n4: Long = graphFacade.addNode(Map("name" -> "bob", "age" -> 50), "person")

    var res: LynxResult = null
    res = graphFacade.cypher("match (n) return n")
    Assert.assertEquals(4, res.records.size)

    res = graphFacade.cypher("match (n) where n.name='alex' return n")
    Assert.assertEquals(1, res.records.size)

    res = graphFacade.cypher("match (n) where n.age=20 return n")
    Assert.assertEquals(1, res.records.size)

    // test index
    graphFacade.createIndexOnNode("person", Set("name"))
    val indexId = graphFacade.hasIndex(Set("person"), Set("name")).map(_._1).getOrElse(0)
    graphFacade.writeNodeIndexRecord(indexId, n1, "bob")
    res = graphFacade.cypher("match (n) where n.name='bob' return n")
    Assert.assertEquals(2, res.records.size)

    res = graphFacade.cypher("match (n:person) where n.name='alex' return n")
    res.show()
    Assert.assertEquals(1, res.records.size)

    graphFacade.writeNodeIndexRecord(indexId, n4, "bob")
    res = graphFacade.cypher("match (n:person) where n.name='bob' return n")
    res.show()
    Assert.assertEquals(2, res.records.size)
  }

  @Test
  def testQueryRelations(): Unit = {
    graphFacade.addRelation("knows", 1L, 2L, Map())
    graphFacade.addRelation("knows", 2L, 3L, Map())
  }


  def printNode(node: LynxNode): Unit ={
    println(s"id = ${node.id.value}, labels = ${node.labels}, name = ${node.property("name").get.value}, age = ${node.property("age").get.value}")
  }

  @Test
  def testQueryRelation(): Unit = {
    val n1: Long = graphFacade.addNode(Map("name" -> "bob", "age" -> 40), "person")
    val n2: Long = graphFacade.addNode(Map("name" -> "alex", "age" -> 20), "person")
    val n3: Long = graphFacade.addNode(Map("name" -> "simba", "age" -> 10), "worker")
    graphFacade.addRelation("friend", 1L, 2L, Map())
    val res = graphFacade.cypher("match (n:person)  return n")
    res.records.toSeq.map(_.apply("n").asInstanceOf[LynxNode]).foreach(printNode)
  }

  @Test
  def testCreate(): Unit ={
    val res = graphFacade.cypher("create (n:person{name:'joejoe'}) ")
    val res2 = graphFacade.cypher("match (n:person)  return n")
    res.records.toSeq.map(_.apply("n").asInstanceOf[LynxNode]).foreach(printNode)
  }

  @Test
  def testCreateRel(): Unit ={
    val res = graphFacade.cypher("CREATE (n:person {name: 'bluejoe', age: 40}),(m:test {name: 'alex', age: 30}),(n)-[:fans]->(m)")
    val res2 = graphFacade.cypher("match (n:person)-[r:fans]->(m: test) where n.name='bluejoe' and m.age=30 return n,r,m")
    res2.show()
  }

  @Test
  def testIO(): Unit = {
    val v = AnyValue(10)
    val s = v.anyValue.isInstanceOf[Int]
    val s2 = v.anyValue.isInstanceOf[Double]
    println(s)
    println(s2)
    val as = Array("test"->1, "test"->2, "jkl" ->3, "jkl"->4)
    val sfg = Array("test")
    val a2 = as.groupBy(row => sfg.map(_.toString))
  }

  def indexDataPrepare(): Unit ={
    val createCypher = Array(
      "create (n:person{name: 'bob', age: 31})",
      "create (n:person{name: 'alice', age: 31.5})",
      "create (n:person{name: 'bobobobob', age: 3})",
      "create (n:person{name: 22, age: 31})",
      "create (n:person{name: 'b', age: -100})",
      "create (n:person{name: 'ba', age: -100.000001})",
      "create (n:person{name: 'bob', age: 32})",
      "create (n:worker{name: 'alice', age: 31.5})",
      "create (n:worker{name: 'bobobobob', age: 3})",
      "create (n:worker{name: 22, age: 31})",
      "create (n:worker{name: 'b', age: -100})",
      "create (n:worker{name: 'ba', age: -100.000001})",
    )
    createCypher.foreach{
      c=>
        graphFacade.cypher(c).show()
    }

  }

  @Test
  def createIndexByAPI(): Unit ={
    indexDataPrepare()
    graphFacade.createIndexOnNode("person", Set("age"))
    println(nodeStore.allPropertyKeys().mkString(","))
    println(nodeStore.allLabels().mkString(","))

    val matchCypher = Array(
      "match (n:person) where n.name = 'bob' return n",
//      "match (n:person) where n.name = 22 return n",
//      "match (n:person) where n.name = 'alice' return n",
//      "match (n:person) where n.age = 31 return n",
      "match (n:person) where n.age < 31 return n",
//      "match (n:person) where n.age <= 31 return n",
//      "match (n:person) where n.age <= 31.00 return n",
//      "match (n:person) where n.age > -100 return n",
//      "match (n:person) where n.age > -100.0000001 return n",
    )

    matchCypher.foreach{
      c=>
        println(c)
        graphFacade.cypher(c).show()
    }

    val indexId = indexStore.getIndexId(nodeStore.getLabelId("person").get,
      Array(nodeStore.getPropertyKeyId("age").get)).get
    graphFacade.close()
  }

  @Test
  def createIndexByCypher(): Unit ={
    indexDataPrepare()
    graphFacade.cypher("CREATE INDEX ON :person(age)")
    val matchCypher = Array(
      "match (n:person) where n.name = 'bob' return n",
      "match (n:person) where n.age < 31 return n"
    )

    matchCypher.foreach{
      c=>
        println(c)
        graphFacade.cypher(c).show()
    }

    val labelId = nodeStore.getLabelId("person")
    val propIds = Array(nodeStore.getPropertyKeyId("age").get)
    val indexId = indexStore.getIndexId(labelId.get, propIds).get
    println(indexId, labelId, propIds.toSet)
    indexStore.findFloatRange(indexId, 0, 100).foreach(println)
//    graphFacade.close()
  }

  def showAll(db: RocksDB): Unit = {
    val it = db.newIterator()
    it.seek(Array[Byte](0))
    while (it.isValid){
      val key = it.key()
      println(key.map(b=>b.toHexString).toList)
      it.next()
    }
  }

  @Test
  def showAll(): Unit ={
    val path = "./testdata/index"
  }

  @Test
  def relTest(): Unit ={
    val types = Seq("label0")
    val label1 = Seq("label1")
    val label2 = Seq("label2")
    graphFacade.rels(label1, types, label2, false, false)
    graphFacade.rels(label1, Seq(), label2, false, false)
    graphFacade.rels(label1, types, Seq(), false, false)
    graphFacade.rels(label1, Seq(), Seq(), false, false)
    graphFacade.rels(Seq(), types, label2, false, false)
    graphFacade.rels(Seq(), Seq(), label2, false, false)
    graphFacade.rels(Seq(), types, Seq(), false, false)
    graphFacade.rels(Seq(), Seq(), Seq(), false, false)
  }
}
