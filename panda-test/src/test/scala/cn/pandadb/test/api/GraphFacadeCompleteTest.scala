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
import org.junit.{After, Assert, Before, Test}

import scala.io.Source

class GraphFacadeCompleteTest {

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

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
  }

  @Test
  def testNodeAddAndGet(): Unit ={
    val longTextFile = Source.fromFile(new File("./testdata/longText"))
    val longText = longTextFile.getLines().next()
    val url = "https://www.baidu.com/img/flexible/logo/pc/result.png"
    val surl = new URL(url)

    graphFacade.addNode(
      Map("text"->longText, "blob"->BlobFactory.fromHttpURL(url),
        "int"->2147483647, "long"->2147483648L, "double"->233.3, "boolean"->true,
        "intArray"->Array[Int](1,2,3),"stringArray"->Array[String]("aa", "bb", "cc"), "longArray"->Array[Long](2147483648L, 2147483649L, 21474836488L)))

    //    graphFacade.addNode(Map(), "person")
//    graphFacade.addNode(Map(), "person", "singer", "fighter", "star")

    val props = graphFacade.nodeAt(1).get.properties
    Assert.assertEquals(longText, props("text").value)
    Assert.assertArrayEquals(IOUtils.toByteArray(surl), props("blob").asInstanceOf[Blob].toBytes())
    Assert.assertEquals(2147483647, props("int").value)
    Assert.assertEquals(2147483648L, props("long").value)
    Assert.assertEquals(233.3, props("double").value)
    Assert.assertEquals(true, props("boolean").value)
    Assert.assertArrayEquals(Array(1,2,3), props("intArray").value.asInstanceOf[Array[Int]])
    Assert.assertEquals(Array("a", "b", "c").toSet, props("stringArray").value.asInstanceOf[Array[String]].toSet)
    Assert.assertArrayEquals(Array(2147483648L, 2147483649L, 21474836488L), props("longArray").value.asInstanceOf[Array[Long]])

  }

  @After
  def close(): Unit = {
    graphFacade.close()
  }
}
