package cn.pandadb

import java.io.File

import cn.pandadb.pnode.store.{FileBasedIdGen, LabelStore, LogStore, NodeStore, RelationStore}
import cn.pandadb.pnode.{GraphFacade, SimpleGraphRAM, PropertiesOp, TypedId}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}

import scala.collection.mutable

class StoreTest {
  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
    new File("./testdata/output/nodes").createNewFile()
    new File("./testdata/output/nodelabels").createNewFile()
    new File("./testdata/output/rellabels").createNewFile()
    new File("./testdata/output/rels").createNewFile()
    new File("./testdata/output/logs").createNewFile()
  }

  @Test
  def test1(): Unit = {
    val nodes = new NodeStore(new File("./testdata/output/nodes"))
    val rels = new RelationStore(new File("./testdata/output/rels"))
    val logs = new LogStore(new File("./testdata/output/logs"))

    val memGraph = new GraphFacade(nodes, rels, logs,
      new LabelStore(new File("./testdata/output/nodelabels")),
      new LabelStore(new File("./testdata/output/rellabels")),
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      new SimpleGraphRAM(),
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
    val nodes = new NodeStore(new File("./testdata/output/nodes"))
    val rels = new RelationStore(new File("./testdata/output/rels"))
    val logs = new LogStore(new File("./testdata/output/logs"))

    val memGraph = new GraphFacade(nodes, rels, logs,
      new LabelStore(new File("./testdata/output/nodelabels")),
      new LabelStore(new File("./testdata/output/rellabels")),
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      new SimpleGraphRAM(),
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

    val res = memGraph.cypher("match (n) return n")
    res.show

    memGraph.close()
  }

  @Test
  def testLabelStore(): Unit ={
    val nodes = new NodeStore(new File("./testdata/output/nodes"))
    val rels = new RelationStore(new File("./testdata/output/rels"))
    val logs = new LogStore(new File("./testdata/output/logs"))
    val nodeLabelStore = new LabelStore(new File("./testdata/output/nodelabels"))
    val relLabelStore = new LabelStore(new File("./testdata/output/rellabels"))
    val memGraph = new GraphFacade(nodes, rels, logs, nodeLabelStore, relLabelStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      new SimpleGraphRAM(),
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

    memGraph.addNode(Map("Name"->"google"), "Person1")
    memGraph.addNode(Map("Name"->"baidu"), "Person2")
    memGraph.addNode(Map("Name"->"android"), "Person3")
    memGraph.addNode(Map("Name"->"ios"), "Person4")
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
}