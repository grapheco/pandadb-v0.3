package cn.pandadb.test.api.graphfacade

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxList, LynxValue}
import org.junit.{After, Assert, Before, Test}

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import scala.io.Source

class NodeAPI {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  val longTextFile = Source.fromFile(new File("./testdata/longText"))
  val longText = longTextFile.getLines().next()

  var nodeId1: Long = _
  var nodeId2: Long = _
  var nodeId3: Long = _

  @Before
  def init(): Unit = {
    val dbPath = "./testdata/testdb/test.db"

    FileUtils.deleteDirectory(new File("./testdata/testdb"))
    FileUtils.forceMkdir(new File(dbPath))

    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )

    nodeId1 = graphFacade.addNode(
      Map("string"->longText, "int"->2147483647, "long"->2147483648L, "double"->233.3, "boolean"->true,
        "intArray"->Array[Int](1,2,3),
        "stringArray"->Array[String]("aa", "bb", "cc"),
        "longArray"->Array[Long](2147483648L, 2147483649L, 21474836488L),
        "booleanArray"->Array[Boolean](true, false, true),
        "doubleArray"->Array[Double](1.1, 2.2, 55555555.5)))
    nodeId2 = graphFacade.addNode(Map(), "person")
    nodeId3 = graphFacade.addNode(Map(), "person", "singer", "fighter", "star")
  }

  @Test
  def testGetNodeProperty(): Unit ={
    val n1 = graphFacade.nodeAt(nodeId1).get
    val n2 = graphFacade.nodeAt(nodeId2).get
    val n3 = graphFacade.nodeAt(nodeId3).get

    Assert.assertEquals(Seq(), n1.labels)
    Assert.assertEquals(longText, n1.properties("string").value)
    Assert.assertEquals(2147483647L, n1.properties("int").value)
    Assert.assertEquals(2147483648L, n1.properties("long").value)
    Assert.assertEquals(233.3, n1.properties("double").value)
    Assert.assertEquals(true, n1.properties("boolean").value)
    Assert.assertEquals(List(1,2,3), n1.properties("intArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(List("aa", "bb", "cc"), n1.properties("stringArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(List(2147483648L, 2147483649L, 21474836488L), n1.properties("longArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(List(true, false, true), n1.properties("booleanArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(List(1.1, 2.2, 55555555.5), n1.properties("doubleArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)

    Assert.assertEquals(List("person"), n2.labels.toList)
    Assert.assertEquals(List("person", "singer", "fighter", "star"), n3.labels.toList)

    val n4 = graphFacade.getNodesByLabels(Seq("person"), false)
    Assert.assertEquals(2, n4.size)
    val n5 = graphFacade.getNodesByLabels(Seq("person1"), false)
    Assert.assertEquals(0, n5.size)
    val n6 = graphFacade.getNodesByLabels(Seq("person"), true)
    Assert.assertEquals(1, n6.size)
    val n7 = graphFacade.getNodesByLabels(Seq(),false)
    Assert.assertEquals(3, n7.size)
  }

  @Test
  def testUpdateNodeProperty(): Unit ={
    graphFacade.nodeRemoveProperty(nodeId1, "not exist")
    graphFacade.nodeAddLabel(nodeId1, "haha")
    val n1 = graphFacade.nodeAt(nodeId1).get
    Assert.assertEquals(longText, n1.properties("string").value)
    Assert.assertEquals(2147483647L, n1.properties("int").value)
    Assert.assertEquals(2147483648L, n1.properties("long").value)
    Assert.assertEquals(233.3, n1.properties("double").value)
    Assert.assertEquals(true, n1.properties("boolean").value)
    Assert.assertEquals(List(1,2,3), n1.properties("intArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(List("aa", "bb", "cc"), n1.properties("stringArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(List(2147483648L, 2147483649L, 21474836488L), n1.properties("longArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(List(true, false, true), n1.properties("booleanArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(List(1.1, 2.2, 55555555.5), n1.properties("doubleArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toList)
    Assert.assertEquals(None, nodeStore.getPropertyKeyId("not exist"))

    graphFacade.nodeSetProperty(nodeId1, "newValue", Long.MinValue - 1)
    val n3 = graphFacade.nodeAt(nodeId1).get

    Assert.assertEquals(n3.properties("newValue"), LynxValue(Long.MaxValue))
  }

  @Test
  def testUpdateNodeLabel(): Unit ={
    graphFacade.nodeAddLabel(nodeId1, "first1")
    graphFacade.nodeAddLabel(nodeId1, "first1")
    graphFacade.nodeAddLabel(nodeId1, "first1")
    graphFacade.nodeAddLabel(nodeId1, "first2")

    graphFacade.nodeRemoveLabel(nodeId1, "not exist")
    Assert.assertEquals(None, nodeStore.getLabelId("not exist"))

    val n1 = graphFacade.nodeAt(nodeId1).get
    Assert.assertEquals(List("first1","first2"), n1.labels.toList)

    val res = graphFacade.nodeHasLabel(nodeId1, "not exist")
    Assert.assertEquals(false, res)
  }

  @Test
  def testDeleteNode(): Unit ={
    graphFacade.deleteNode(nodeId1)
    graphFacade.deleteNode(nodeId1)
    val res2 = graphFacade.nodeAt(nodeId1)
    Assert.assertEquals(None, res2)
  }

  @Test
  def testNodeIterator(): Unit ={
    graphFacade.nodes().foreach(node => Assert.assertEquals(graphFacade.nodeAt(node.id).get, node))
  }

  @After
  def close(): Unit = {
    graphFacade.close()
  }
}



