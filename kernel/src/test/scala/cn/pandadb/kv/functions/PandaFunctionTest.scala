package cn.pandadb.kv.functions

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxDate, LynxInteger, LynxValue}
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
    Assert.assertEquals(100L, res.values.head.asInstanceOf[LynxValue].value)
  }

  @Test
  def toDateTest(): Unit ={
    val res = graphFacade.cypher("return date('2020-03-12')").records().next()
    Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-12").getTime ,
      res.values.head.asInstanceOf[LynxValue].value)
  }

  @Test
  def relationTypeTest(): Unit ={
    val res = graphFacade.cypher(
      """
        |create (n:person{name:'a'})
        |create (m:person{name:'b'})
        |create (n)-[r:known]->(m) return type(r)
        |""".stripMargin).records().next()
    Assert.assertEquals("known", res.values.head.asInstanceOf[LynxValue].value)
  }

  @Test
  def existsTest(): Unit ={
    graphFacade.cypher(
      """
        |create (n:person{name:'a', age:10})
        |create (m:person{name:'b'})
        |""".stripMargin)
    val res = graphFacade.cypher("match (n) return exists(n.age)").records()
    val r1 = res.next().values.head.asInstanceOf[LynxValue].value
    val r2 = res.next().values.head.asInstanceOf[LynxValue].value
    Assert.assertEquals(true, r1)
    Assert.assertEquals(false, r2)
  }

  @Test
  def orderByTest(): Unit ={
    graphFacade.cypher(
      """
        |create (n1:person{name:'a', age:13})
        |create (n2:person{name:'b', age:11})
        |create (n3:person{name:'c', age:12})
        |""".stripMargin)

    val res = graphFacade.cypher("match (n) return n.age order by n.age asc").records()
    val n1 = res.next().values.head.asInstanceOf[LynxValue].value
    val n2 = res.next().values.head.asInstanceOf[LynxValue].value
    val n3 = res.next().values.head.asInstanceOf[LynxValue].value
    Assert.assertEquals(11, n1)
    Assert.assertEquals(12, n2)
    Assert.assertEquals(13, n3)


  }
  @Test
  def idTest(): Unit ={
    graphFacade.cypher(
      """
        |create (n1:person{name:'a', age:13})
        |create (n2:person{name:'b', age:11})
        |create (n1)-[r:known]->(n2)
        |""".stripMargin)

    val res1 = graphFacade.cypher("match (n) return id(n)").records()
    val res2 = graphFacade.cypher("match ()-[r]->() return id(r)").records().next().values.head.asInstanceOf[LynxValue].value

    val n1 = res1.next().values.head.asInstanceOf[LynxValue].value
    val n2 = res1.next().values.head.asInstanceOf[LynxValue].value
    Assert.assertEquals(1L, n1)
    Assert.assertEquals(2L, n2)
    Assert.assertEquals(1L, res2)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
