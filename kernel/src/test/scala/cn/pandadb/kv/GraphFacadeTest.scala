//package cn.pandadb.kv
//
//import java.io.File
//
//import cn.pandadb.kernel.kv.{GraphFacade, RocksDBGraphAPI}
//import cn.pandadb.kernel.store.{FileBasedIdGen, LabelStore, StoredNodeWithProperty_tobe_deprecated}
//import org.apache.commons.io.FileUtils
//import org.junit.{After, Assert, Before, Test}
//import org.opencypher.okapi.api.graph.CypherResult
//import org.opencypher.okapi.api.value.CypherValue.{Node, Relationship}
//
//class GraphFacadeTest {
//
//  var nodeLabelStore: LabelStore = _
//  var relLabelStore: LabelStore = _
//  var graphStore: RocksDBGraphAPI = _
//
//  var graphFacade: GraphFacade = _
//
//
//  @Before
//  def setup(): Unit = {
//    FileUtils.deleteDirectory(new File("./testdata/output"))
//    new File("./testdata/output").mkdirs()
//    new File("./testdata/output/nodelabels").createNewFile()
//    new File("./testdata/output/rellabels").createNewFile()
//
//    nodeLabelStore = new LabelStore(new File("./testdata/output/nodelabels"))
//    relLabelStore = new LabelStore(new File("./testdata/output/rellabels"))
//    graphStore = new RocksDBGraphAPI("./testdata/output/rocksdb")
//
//    graphFacade = new GraphFacade( nodeLabelStore, relLabelStore,
//      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
//      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
//      graphStore,
//      {}
//    )
//  }
//
//  @Test
//  def test1(): Unit = {
//    Assert.assertEquals(0, graphStore.allNodes().size)
//    Assert.assertEquals(0, graphStore.allRelations().size)
//
//    graphFacade.addNode(Map("name" -> "1")).addNode(Map("name" -> "2")).addRelation("1->2", 1, 2, Map())
//
//    //nodes: {1,2}
//    //rels: {1}
//    Assert.assertEquals(List(1, 2), graphStore.allNodes().toSeq.map(_.id).sorted)
//
//    graphFacade.addNode(Map("name" -> "3"))
//    //nodes: {1,2,3}
//    Assert.assertEquals(List(1, 2, 3), graphStore.allNodes().toSeq.map(_.id).sorted)
//
//    graphFacade.deleteNode(2)
//    //nodes: {1,3}
//    Assert.assertEquals(List(1, 3), graphStore.allNodes().toSeq.map(_.id).sorted)
//
//    graphFacade.addNode(Map("name" -> "4")).deleteNode(1L)
//    //nodes: {3,4}
//    Assert.assertEquals(List(3, 4), graphStore.allNodes().toSeq.map(_.id).sorted)
//
//    graphFacade.addNode(Map("name" -> "5")).addNode(Map("name" -> "6")).deleteNode(5L).deleteNode(3L)
//    //nodes: {4,6}
//    Assert.assertEquals(List(4, 6), graphStore.allNodes().toSeq.map(_.id).sorted)
//
//    graphFacade.close()
//  }
//
//  @Test
//  def testQuery(): Unit = {
//    graphFacade.addNode(Map("name" -> "bluejoe", "age" -> 40), "person")
//    graphFacade.addNode(Map("name" -> "alex", "age" -> 20), "person")
//    graphFacade.addNode(Map("name" -> "simba", "age" -> 10), "person")
//    graphFacade.addRelation("knows", 1L, 2L, Map())
//    graphFacade.addRelation("knows", 2L, 3L, Map())
//
//    var res: CypherResult = null
//    res = graphFacade.cypher("match (n) return n")
//    res.show
//    Assert.assertEquals(3, res.records.size)
//
//    res = graphFacade.cypher("match (n) where n.name='alex' return n")
//    res.show
//    Assert.assertEquals(1, res.records.size)
//
//    res = graphFacade.cypher("match (n) where n.age=20 return n")
//    res.show
//    Assert.assertEquals(1, res.records.size)
//
//    res = graphFacade.cypher("match (n) where n.age>18 return n")
//    res.show
//    Assert.assertEquals(2, res.records.size)
//
//    res = graphFacade.cypher("match (m)-[r]->(n) return m,r,n")
//    res.show
//    val rec = res.records.collect
//    Assert.assertEquals(2, rec.size)
//
//    Assert.assertEquals(1, rec.apply(0).apply("m").cast[Node[Long]].id)
//    Assert.assertEquals(2, rec.apply(0).apply("n").cast[Node[Long]].id)
//    Assert.assertEquals(1, rec.apply(0).apply("r").cast[Relationship[Long]].id)
//
//    Assert.assertEquals(2, rec.apply(1).apply("m").cast[Node[Long]].id)
//    Assert.assertEquals(3, rec.apply(1).apply("n").cast[Node[Long]].id)
//    Assert.assertEquals(2, rec.apply(1).apply("r").cast[Relationship[Long]].id)
//
//    graphFacade.close()
//  }
//
//  @Test
//  def testLabelStore(): Unit = {
//
//    graphFacade.addNode(Map("Name" -> "google"), "Person1")
//    graphFacade.addNode(Map("Name" -> "baidu"), "Person2")
//    graphFacade.addNode(Map("Name" -> "android"), "Person3")
//    graphFacade.addNode(Map("Name" -> "ios"), "Person4")
//    graphFacade.addRelation("relation1", 1L, 2L, Map())
//    graphFacade.addRelation("relation2", 2L, 3L, Map())
//
//    Assert.assertEquals(4, nodeLabelStore.map.seq.size)
//    Assert.assertEquals(2, relLabelStore.map.seq.size)
//
//    graphFacade.close()
//
//    val nodeLabelStore2 = new LabelStore(new File("./testdata/output/nodelabels"))
//    val relLabelStore2 = new LabelStore(new File("./testdata/output/rellabels"))
//
//    Assert.assertEquals(4, nodeLabelStore2._store.loadAll().size)
//    Assert.assertEquals(2, relLabelStore2._store.loadAll().size)
//
//  }
//
//  @Test
//  def testRelationStore(): Unit = {
//
//    graphFacade.addNode(Map("Name" -> "google"), "Person1")
//    graphFacade.addNode(Map("Name" -> "baidu"), "Person2")
//    graphFacade.addNode(Map("Name" -> "android"), "Person3")
//    graphFacade.addNode(Map("Name" -> "ios"), "Person4")
//    graphFacade.addRelation("relation1", 1L, 2L, Map())
//    graphFacade.addRelation("relation2", 2L, 3L, Map())
//    graphFacade.addRelation("relation3", 3L, 4L, Map())
//
//    Assert.assertEquals(4, graphStore.allNodes().size)
//    Assert.assertEquals(3, graphStore.allRelations().size)
//
//    graphFacade.deleteRelation(1)
//
//    Assert.assertEquals(2, graphStore.allRelations().size)
//    graphFacade.close()
//  }
//
//
//  @Test
//  def testAddNode(): Unit = {
//    graphFacade.addNode(Map("Name" -> "google"), "Person1")
//    Assert.assertEquals(1, graphStore.allNodes().size)
//    graphFacade.close()
//  }
//
//  @Test
//  def testNodeFilterByLabel(): Unit = {
//    graphFacade.addNode(Map("Name" -> "google"), "company")
//    graphFacade.addNode(Map("Name" -> "fb"), "company", "person")
//    graphFacade.addNode(Map("Name" -> "microsoft"), "person")
//
//    Assert.assertEquals(3, graphStore.allNodes().size)
//    val res1 = graphStore.findNodes(nodeLabelStore.id("person"))
//    Assert.assertEquals(2, res1.size)
//
//    val res2 = graphFacade.cypher("match (n:person) return n")
//    res2.show
//    Assert.assertEquals(2, res2.records.size)
//
//    graphFacade.close()
//
//  }
//
//  @Test
//  def testCreateNodeByCypher(): Unit = {
//    graphFacade.cypher("create (n:person) return n")
//    graphFacade.cypher("create (n:person{name:'test1',age:20, work: 'google'})")
//    graphFacade.cypher("create (n:person{name:'test2',age:22, job: 'dev'})")
//
//    Assert.assertEquals(2, graphFacade.allNodes().size)
//
//    graphFacade.close()
//  }
//
//}
