package cn.pandadb.kv.functions

import java.io.File

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxDate, LynxInteger}
import org.junit.{After, Assert, Before, Test}

class PandaFunctionTest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  @Before
  def init(): Unit ={
    val dbPath = "./functionTest.db"
    val file = new File(dbPath)
    if (file.exists()){
      FileUtils.deleteDirectory(file)
    }

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
  def toIntegerTest(): Unit ={
    val res = graphFacade.cypher("return toInteger('100')").records().next()
    Assert.assertEquals(true, res.values.head.isInstanceOf[LynxInteger])
  }

  @Test
  def toDateTest(): Unit ={
    val res = graphFacade.cypher("return date('2020-03-12')").records().next()
    println(res)
    Assert.assertEquals(true, res.values.head.isInstanceOf[LynxDate])
  }

  @Test
  def relationTypeTest(): Unit ={
    graphFacade.cypher(
      """
        |create (n:person{name:'a'})
        |create (m:person{name:'b'})
        |create (n)-[r:known]->(m) return type(r)
        |""".stripMargin).show()
  }
  @Test
  def countTest(): Unit ={
    graphFacade.cypher(
      """
        |create (n:person{name:'a'})
        |create (m:person{name:'b'})
        |""".stripMargin)
    graphFacade.cypher("match (n) return count(n)").show()
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
