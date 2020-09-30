import java.io.File

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

    Assert.assertEquals(0, nodes.list().size)
    Assert.assertEquals(0, rels.list().size)
    Assert.assertEquals(0, logs.list().size)

    logs.append(CreateNode(Node(1)))
    logs.append(CreateNode(Node(2)))
    logs.append(CreateRelation(Relation(1, 1, 2)))

    Assert.assertEquals(3, logs.list().size)
    Assert.assertEquals(0, nodes.list().size)
    Assert.assertEquals(0, rels.list().size)

    //flush now
    logs.flush(nodes, rels)

    Assert.assertEquals(0, logs.list().size)
    Assert.assertEquals(2, nodes.list().size)
    Assert.assertEquals(1, rels.list().size)

    Assert.assertEquals(Node(1), nodes.list()(0))
    Assert.assertEquals(Node(2), nodes.list()(1))
    Assert.assertEquals(Relation(1, 1, 2), rels.list()(0))
  }
}