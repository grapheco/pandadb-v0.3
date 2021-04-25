package cn.pandadb.test.api.graphfacade

import java.io.File

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.junit.{After, Before}

import scala.io.Source

class IndexApi {
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

  @After
  def close(): Unit = {
    graphFacade.close()
  }
}
