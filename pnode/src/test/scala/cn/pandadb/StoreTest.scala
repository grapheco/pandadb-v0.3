package cn.pandadb

import java.io.File

import cn.pandadb.pnode.store.{FileBasedIdGen, FileBasedLogStore, FileBasedNodeStore, FileBasedRelationStore}
import cn.pandadb.pnode.{GraphFacade, MemGraphOp, PropertiesOp, TypedId}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}

import scala.collection.mutable

class StoreTest {
  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
    new File("./testdata/output/nodes").createNewFile()
    new File("./testdata/output/rels").createNewFile()
    new File("./testdata/output/logs").createNewFile()
  }

  @Test
  def test1(): Unit = {
    val nodes = new FileBasedNodeStore(new File("./testdata/output/nodes"))
    val rels = new FileBasedRelationStore(new File("./testdata/output/rels"))
    val logs = new FileBasedLogStore(new File("./testdata/output/logs"))

    val memGraph = new GraphFacade(nodes, rels, logs,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      new MemGraphOp(),
      new PropertiesOp {
        val propStore = mutable.Map[TypedId, mutable.Map[String, Any]]()

        override def create(id: TypedId, props: Map[String, Any]): Unit =
          propStore += id -> (mutable.Map[String, Any]() ++ props)

        override def delete(id: TypedId): Unit = propStore -= id

        override def lookup(id: TypedId): Option[Map[String, Any]] = propStore.get(id).map(_.toMap)

        override def close(): Unit = {
        }
      }, {

      }
    )

    Assert.assertEquals(0, nodes.list().size)
    Assert.assertEquals(0, rels.list().size)
    Assert.assertEquals(0, logs.list().size)

    memGraph.addNode(Map("name" -> "bluejoe")).addNode(Map("name" -> "alex")).addRelation("knows", 1, 2, Map())

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

    memGraph.close()
  }
}