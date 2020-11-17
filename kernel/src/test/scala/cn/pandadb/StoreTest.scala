package cn.pandadb

import java.io.File

import cn.pandadb.kernel.impl.{SimpleGraphRAM, SimplePropertyStore}
import cn.pandadb.kernel.store.{FileBasedIdGen, LabelStore, LogStore, NodeStore, RelationStore}
import cn.pandadb.kernel.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue.{Node, Relationship}

class StoreTest {
  var nodes: NodeStore = _
  var rels: RelationStore = _
  var logs: LogStore = _
  var nodeLabelStore: LabelStore = _
  var relLabelStore: LabelStore = _
  var memGraph: GraphFacade = _

  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
    new File("./testdata/output/nodes").createNewFile()
    new File("./testdata/output/nodelabels").createNewFile()
    new File("./testdata/output/rellabels").createNewFile()
    new File("./testdata/output/rels").createNewFile()
    new File("./testdata/output/logs").createNewFile()

    nodes = new NodeStore(new File("./testdata/output/nodes"))
    rels = new RelationStore(new File("./testdata/output/rels"))
    logs = new LogStore(new File("./testdata/output/logs"))
    nodeLabelStore = new LabelStore(new File("./testdata/output/nodelabels"))
    relLabelStore = new LabelStore(new File("./testdata/output/rellabels"))

    memGraph = new GraphFacade(nodes, rels, logs, nodeLabelStore, relLabelStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
//      new DirectGraphRAMImpl(),
      new SimpleGraphRAM(),
      new SimplePropertyStore {

      }, {

      }
    )

  }

  @Test
  def test1(): Unit = {
    Assert.assertEquals(0, nodes.loadAll().size)
    Assert.assertEquals(0, rels.loadAll().size)
    Assert.assertEquals(0, logs._store.loadAll().size)

    memGraph.addNode(Map("name" -> "1")).addNode(Map("name" -> "2")).addRelation("1->2", 1, 2, Map())

    //nodes: {1,2}
    //rels: {1}
    Assert.assertEquals(3, logs._store.loadAll().size)
    Assert.assertEquals(0, nodes.loadAll().size)
    Assert.assertEquals(0, rels.loadAll().size)

    memGraph.mergeLogs2Store(true)

    Assert.assertEquals(0, logs._store.loadAll().size)
    Assert.assertEquals(List(1, 2), nodes.loadAll().map(_.id).sorted)
    Assert.assertEquals(List(1), rels.loadAll().map(_.id).sorted)

    memGraph.addNode(Map("name" -> "3"))
    //nodes: {1,2,3}
    memGraph.mergeLogs2Store(true)

    Assert.assertEquals(0, logs._store.loadAll().size)
    Assert.assertEquals(List(1, 2, 3), nodes.loadAll().map(_.id).sorted)

    memGraph.deleteNode(2)
    //nodes: {1,3}
    memGraph.mergeLogs2Store(true)
    Assert.assertEquals(List(1, 3), nodes.loadAll().map(_.id).sorted)

    memGraph.addNode(Map("name" -> "4")).deleteNode(1L)
    //nodes: {3,4}
    memGraph.mergeLogs2Store(true)
    Assert.assertEquals(List(3, 4), nodes.loadAll().map(_.id).sorted)

    memGraph.addNode(Map("name" -> "5")).addNode(Map("name" -> "6")).deleteNode(5L).deleteNode(3L)
    //nodes: {4,6}
    memGraph.mergeLogs2Store(true)
    Assert.assertEquals(List(4, 6), nodes.loadAll().map(_.id).sorted)

    memGraph.close()
  }

  @Test
  def testQuery(): Unit = {
    memGraph.addNode(Map("Name" -> "bluejoe", "age" -> 40), "person")
    memGraph.addNode(Map("Name" -> "alex", "age" -> 20), "person")
    memGraph.addNode(Map("Name" -> "simba", "age" -> 10), "person")
    memGraph.addRelation("knows", 1L, 2L, Map())
    memGraph.addRelation("knows", 2L, 3L, Map())

    var res: CypherResult = null
    res = memGraph.cypher("match (n) return n")
    res.show
    Assert.assertEquals(3, res.records.size)

    res = memGraph.cypher("match (m)-[r]->(n) return m,r,n")
    res.show
    val rec = res.records.collect
    Assert.assertEquals(2, rec.size)

    Assert.assertEquals(1, rec.apply(0).apply("m").cast[Node[Long]].id)
    Assert.assertEquals(2, rec.apply(0).apply("n").cast[Node[Long]].id)
    Assert.assertEquals(1, rec.apply(0).apply("r").cast[Relationship[Long]].id)

    Assert.assertEquals(2, rec.apply(1).apply("m").cast[Node[Long]].id)
    Assert.assertEquals(3, rec.apply(1).apply("n").cast[Node[Long]].id)
    Assert.assertEquals(2, rec.apply(1).apply("r").cast[Relationship[Long]].id)

    memGraph.close()
  }

  @Test
  def testLabelStore(): Unit = {

    memGraph.addNode(Map("Name" -> "google"), "Person1")
    memGraph.addNode(Map("Name" -> "baidu"), "Person2")
    memGraph.addNode(Map("Name" -> "android"), "Person3")
    memGraph.addNode(Map("Name" -> "ios"), "Person4")
    memGraph.addRelation("relation1", 1L, 2L, Map())
    memGraph.addRelation("relation2", 2L, 3L, Map())

    Assert.assertEquals(4, nodeLabelStore.map.seq.size)
    Assert.assertEquals(2, relLabelStore.map.seq.size)
    Assert.assertEquals(6, logs._store.loadAll().size)

    memGraph.mergeLogs2Store(true)
    Assert.assertEquals(0, logs._store.loadAll().size)

    memGraph.close()

    val nodeLabelStore2 = new LabelStore(new File("./testdata/output/nodelabels"))
    val relLabelStore2 = new LabelStore(new File("./testdata/output/rellabels"))

    Assert.assertEquals(4, nodeLabelStore2._store.loadAll().size)
    Assert.assertEquals(2, relLabelStore2._store.loadAll().size)

  }

  @Test
  def testRelationStore(): Unit = {

    memGraph.addNode(Map("Name" -> "google"), "Person1")
    memGraph.addNode(Map("Name" -> "baidu"), "Person2")
    memGraph.addNode(Map("Name" -> "android"), "Person3")
    memGraph.addNode(Map("Name" -> "ios"), "Person4")
    memGraph.addRelation("relation1", 1L, 2L, Map())
    memGraph.addRelation("relation2", 2L, 3L, Map())
    memGraph.addRelation("relation3", 3L, 4L, Map())

    memGraph.mergeLogs2Store(true)
    Assert.assertEquals(3, rels.loadAll().size)

    memGraph.deleteRelation(1)
    memGraph.mergeLogs2Store(true)

    Assert.assertEquals(2, rels.loadAll().size)
    memGraph.close()
  }
}