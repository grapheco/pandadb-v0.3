package cn.pandadb.test.api

import java.io.File
import java.net.URL

import cn.pandadb.kernel.blob.api.Blob
import cn.pandadb.kernel.blob.impl.BlobFactory
import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.grapheco.lynx.{LynxList, LynxValue}
import org.junit.{After, Assert, Before, Test}

import scala.io.Source

class GraphFacadeCompleteTest {

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  val longTextFile = Source.fromFile(new File("./testdata/longText"))
  val longText = longTextFile.getLines().next()

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

    graphFacade.addNode(
      Map("string"->longText, "int"->2147483647, "long"->2147483648L, "double"->233.3, "boolean"->true,
        "intArray"->Array[Int](1,2,3),
        "stringArray"->Array[String]("aa", "bb", "cc"),
        "longArray"->Array[Long](2147483648L, 2147483649L, 21474836488L),
        "booleanArray"->Array[Boolean](true, false, true),
        "doubleArray"->Array[Double](1.1, 2.2, 55555555.5)))
    graphFacade.addNode(Map(), "person")
    graphFacade.addNode(Map(), "person", "singer", "fighter", "star")
  }

  @Test
  def testGetNodeProperty(): Unit ={
    val n1 = graphFacade.nodeAt(1).get
    val n2 = graphFacade.nodeAt(2).get
    val n3 = graphFacade.nodeAt(3).get

    Assert.assertEquals(Seq(), n1.labels)
    Assert.assertEquals(longText, n1.properties("string").value)
    Assert.assertEquals(2147483647L, n1.properties("int").value)
    Assert.assertEquals(2147483648L, n1.properties("long").value)
    Assert.assertEquals(233.3, n1.properties("double").value)
    Assert.assertEquals(true, n1.properties("boolean").value)
    Assert.assertEquals(Set(1,2,3), n1.properties("intArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
    Assert.assertEquals(Set("aa", "bb", "cc"), n1.properties("stringArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
    Assert.assertEquals(Set(2147483648L, 2147483649L, 21474836488L), n1.properties("longArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
    Assert.assertEquals(Set(true, false, true), n1.properties("booleanArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
    Assert.assertEquals(Set(1.1, 2.2, 55555555.5), n1.properties("doubleArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)

    Assert.assertEquals(Set("person"), n2.labels.toSet)
    Assert.assertEquals(Set("person", "singer", "fighter", "star"), n3.labels.toSet)
  }

  @Test
  def testUpdateNodeProperty(): Unit ={
    graphFacade.nodeRemoveProperty(1, "not exist")
    val n1 = graphFacade.nodeAt(1).get
    Assert.assertEquals(longText, n1.properties("string").value)
    Assert.assertEquals(2147483647L, n1.properties("int").value)
    Assert.assertEquals(2147483648L, n1.properties("long").value)
    Assert.assertEquals(233.3, n1.properties("double").value)
    Assert.assertEquals(true, n1.properties("boolean").value)
    Assert.assertEquals(Set(1,2,3), n1.properties("intArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
    Assert.assertEquals(Set("aa", "bb", "cc"), n1.properties("stringArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
    Assert.assertEquals(Set(2147483648L, 2147483649L, 21474836488L), n1.properties("longArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
    Assert.assertEquals(Set(true, false, true), n1.properties("booleanArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
    Assert.assertEquals(Set(1.1, 2.2, 55555555.5), n1.properties("doubleArray").asInstanceOf[LynxList].value.toArray.map(f => f.value).toSet)
  }

  @Test
  def testUpdateNodeLabel(): Unit ={
    graphFacade.nodeAddLabel(1, "first1")
    graphFacade.nodeAddLabel(1, "first2")
    graphFacade.nodeRemoveLabel(1, "not exist")

    val n1 = graphFacade.nodeAt(1).get
    Assert.assertEquals(Set("first1","first2"), n1.labels.toSet)
    Assert.assertEquals(None, nodeStore.getLabelId("not exist"))
  }

  @Test
  def testBlobProperty(): Unit ={
    val url = "https://www.baidu.com/img/flexible/logo/pc/result.png"
    val surl = new URL(url)
    graphFacade.addNode(Map("blob"->BlobFactory.fromHttpURL(url)))
    val props = graphFacade.nodeAt(1).get.properties
    Assert.assertArrayEquals(IOUtils.toByteArray(surl), props("blob").asInstanceOf[Blob].toBytes())
  }


  // RELATIONSHIP API TEST


  //INDEX API TEST

  @After
  def close(): Unit = {
    graphFacade.close()
  }
}
