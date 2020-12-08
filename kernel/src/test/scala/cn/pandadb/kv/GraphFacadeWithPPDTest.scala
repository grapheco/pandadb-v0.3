package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.{GraphFacade, GraphFacadeWithPPD, NodeLabelStore, PropertyNameStore, RelationLabelStore, RocksDBGraphAPI, TokenStore}
import cn.pandadb.kernel.store.{FileBasedIdGen, LabelStore}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue.{Node, Relationship}

class GraphFacadeWithPPDTest {

  var nodeLabelStore: TokenStore = _
  var relLabelStore: TokenStore = _
  var propNameStore: TokenStore = _
  var graphStore: RocksDBGraphAPI = _

  var graphFacade: GraphFacadeWithPPD = _


  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
    new File("./testdata/output/nodelabels").createNewFile()
    new File("./testdata/output/rellabels").createNewFile()

    graphStore = new RocksDBGraphAPI("./testdata/output/rocksdb")

    nodeLabelStore = new NodeLabelStore(graphStore.getRocksDB)
    relLabelStore = new RelationLabelStore(graphStore.getRocksDB)
    propNameStore = new PropertyNameStore(graphStore.getRocksDB)




    graphFacade = new GraphFacadeWithPPD( nodeLabelStore, relLabelStore, propNameStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      graphStore,
      {}
    )
  }

  @Test
  def test1(): Unit = {
    Assert.assertEquals(0, graphStore.allNodes().size)
    Assert.assertEquals(0, graphStore.allRelations().size)

    graphFacade.addNode(Map("name" -> "1")).addNode(Map("name" -> "2")).addRelation("1->2", 1, 2, Map())

    //nodes: {1,2}
    //rels: {1}
    Assert.assertEquals(List(1, 2), graphStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.addNode(Map("name" -> "3"))
    //nodes: {1,2,3}
    Assert.assertEquals(List(1, 2, 3), graphStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.deleteNode(2)
    //nodes: {1,3}
    Assert.assertEquals(List(1, 3), graphStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.addNode(Map("name" -> "4")).deleteNode(1L)
    //nodes: {3,4}
    Assert.assertEquals(List(3, 4), graphStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.addNode(Map("name" -> "5")).addNode(Map("name" -> "6")).deleteNode(5L).deleteNode(3L)
    //nodes: {4,6}
    Assert.assertEquals(List(4, 6), graphStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.close()
  }

  @Test
  def testQuery(): Unit = {
    val n1: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 40), "person")
    val n2: Long = graphFacade.addNode2(Map("name" -> "alex", "age" -> 20), "person")
    val n3: Long = graphFacade.addNode2(Map("name" -> "simba", "age" -> 10), "worker")
    val n4: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 50), "person")

    var res: CypherResult = null
    res = graphFacade.cypher("match (n) return n")
    res.show
    Assert.assertEquals(4, res.records.size)

    res = graphFacade.cypher("match (n) where n.name='alex' return n")
    res.show
    Assert.assertEquals(1, res.records.size)

    res = graphFacade.cypher("match (n) where n.age=20 return n")
    res.show
    Assert.assertEquals(1, res.records.size)

    // test index
    val indexId = graphFacade.createNodePropertyIndex("person", Set("name"))
    graphFacade.writeNodeIndexRecord(indexId, n1, "bob")

    res = graphFacade.cypher("match (n:person) where n.name='bob' return n")
    res.show
    Assert.assertEquals(1, res.records.size)

    res = graphFacade.cypher("match (n:person) where n.name='alex' return n")
    res.show
    Assert.assertEquals(0, res.records.size)

    graphFacade.writeNodeIndexRecord(indexId, n4, "bob")
    res = graphFacade.cypher("match (n:person) where n.name='bob' return n")
    res.show
    Assert.assertEquals(2, res.records.size)

    graphFacade.close()
  }


}
