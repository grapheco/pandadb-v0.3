import java.io.File

import cn.pandadb.pnode.MemGraph
import cn.pandadb.pnode.store.{CreateNode, CreateRelation, FileBasedLogStore, FileBasedNodeStore, FileBasedRelationStore, Node, Relation}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}

class StoreTest {
  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testoutput"))
    new File("./testoutput").mkdir()
    new File("./testoutput/nodes").createNewFile()
    new File("./testoutput/rels").createNewFile()
    new File("./testoutput/logs").createNewFile()
  }

  @Test
  def test1(): Unit = {
    val nodes = new FileBasedNodeStore(new File("./testoutput/nodes"))
    val rels = new FileBasedRelationStore(new File("./testoutput/rels"))
    val logs = new FileBasedLogStore(new File("./testoutput/logs"))
    val memGraph = new MemGraph(nodes, rels, logs)

    Assert.assertEquals(0, nodes.list().size)
    Assert.assertEquals(0, rels.list().size)
    Assert.assertEquals(0, logs.list().size)

    memGraph.addNode(Node(1)).addNode(Node(2)).addRelation(Relation(1, 1, 2))

    Assert.assertEquals(3, logs.list().size)
    Assert.assertEquals(0, nodes.list().size)
    Assert.assertEquals(0, rels.list().size)

    //flush now
    memGraph.dumpAll()

    Assert.assertEquals(0, logs.list().size)
    Assert.assertEquals(2, nodes.list().size)
    Assert.assertEquals(1, rels.list().size)

    Assert.assertEquals(1, nodes.list()(0).id)
    Assert.assertEquals(2, nodes.list()(1).id)
    Assert.assertEquals(1, rels.list()(0).id)
  }
}