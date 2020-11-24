package cn.pandadb

import java.io.File

import cn.pandadb.kernel.impl.{SimpleGraphRAM, SimplePropertyStore}
import cn.pandadb.kernel.store.{FileBasedIdGen, LabelStore, LogStore, NodeStore, NodeStoreImpl, RelationStore, RelationStoreImpl}
import cn.pandadb.kernel.{GraphFacade, NodeId, PropertyStore}
import cn.pandadb.kernel.lucene.LucenePropertyStore
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue.{Node, Relationship}

class LuceneStoreTest {
  var nodes: NodeStore = _
  var rels: RelationStore = _
  var logs: LogStore = _
  var nodeLabelStore: LabelStore = _
  var relLabelStore: LabelStore = _
  var propStore: PropertyStore = _
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

    nodes = new NodeStoreImpl(new File("./testdata/output/nodes"), new File("./testdata/output/nodesmap"))
    rels = new RelationStoreImpl(new File("./testdata/output/rels"), new File("./testdata/output/relsmap"))
    logs = new LogStore(new File("./testdata/output/logs"))
    nodeLabelStore = new LabelStore(new File("./testdata/output/nodelabels"))
    relLabelStore = new LabelStore(new File("./testdata/output/rellabels"))
    propStore = new LucenePropertyStore(new File("./testdata/output/properties"))
//    propStore = new SimplePropertyStore()

    memGraph = new GraphFacade(nodes, rels, logs, nodeLabelStore, relLabelStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      //new DirectGraphRAMImpl(),
      new SimpleGraphRAM(),
      propStore,
      { }
    )
  }

  @Test
  def testLunceneProps(): Unit = {
    memGraph.addNode(Map("Name" -> "bluejoe", "age" -> 40), "person")
    memGraph.addNode(Map("Name" -> "alex", "age" -> 20), "person")
    memGraph.addNode(Map("Name" -> "simba", "age" -> 10), "person")
    memGraph.addRelation("knows", 1L, 2L, Map())
    memGraph.addRelation("knows", 2L, 3L, Map())
    memGraph.mergeLogs2Store()
    var res = propStore.lookup(NodeId(1L))
    println(res)
    Assert.assertEquals("1_1", res.get.get("_id").get)
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
    memGraph.mergeLogs2Store()
    res = memGraph.cypher("match (n) where n.name='bluejoe' return n")
    res.show
    Assert.assertEquals(1, res.records.size)
    memGraph.close()
  }

}