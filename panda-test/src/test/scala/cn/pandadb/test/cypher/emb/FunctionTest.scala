package cn.pandadb.test.cypher.emb

import java.io.File

import cn.pandadb.kernel.{GraphDatabaseBuilder, GraphService}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.LynxValue
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: supported cypher functions
 * @author: LiamGao
 * @create: 2021-04-26
 */
class FunctionTest {
  val dbPath = "./testdata/emb"
  var db: GraphService = _

  var nodeId1: Long = _
  var nodeId2: Long = _
  var nodeId3: Long = _

  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File(dbPath))
    FileUtils.forceMkdir(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath)
    prepareData(db)
  }

  def prepareData(db: GraphService): Unit ={
    nodeId1 = db.addNode(Map("name"->"alex",
      "storage"->1000000,
      "salary"->2333.33,
      "isCoder"->true,
      "indexs"->Array[Int](1,2,3,4,5),
      "floats"->Array[Double](11.1, 22.2, 33.3),
      "bools"->Array[Boolean](true, true, false, false),
      "jobs"->Array[String]("teacher", "coder", "singer")), "person")

    nodeId2 = db.addNode(Map("name"->"bob",
      "storage"->1000000,
      "indexs"->Array[Int](1,2,3,4,5)), "people")

    nodeId3 = db.addNode(Map("name"->"clause",
      "storage"->1000000,
      "isCoder"->true,
      "indexs"->Array[Int](1,2,3,4,5)), "person")
  }

  @Test
  def testCount(): Unit ={
    val res1 = db.cypher("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    val res2 = db.cypher("match (n:person) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    val res3 = db.cypher("match (n:people) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value

    Assert.assertEquals(3L, res1)
    Assert.assertEquals(2L, res2)
    Assert.assertEquals(1L, res3)
  }

  @Test
  def testId(): Unit ={
    val res1 = db.cypher("match (n) return id(n)").records().toList.map(f => f("id(n)").asInstanceOf[LynxValue].value).toSet
    val res2 = db.cypher("match (n:person) return id(n)").records().toList.map(f => f("id(n)").asInstanceOf[LynxValue].value).toSet
    val res3 = db.cypher("match (n:people) return id(n)").records().toList.map(f => f("id(n)").asInstanceOf[LynxValue].value).toSet

    Assert.assertEquals(Set(nodeId1, nodeId2, nodeId3), res1)
    Assert.assertEquals(Set(nodeId1, nodeId3), res2)
    Assert.assertEquals(Set(nodeId2), res3)
  }

  @Test
  def testType(): Unit ={
    val res1 = db.cypher("match (n) return type(n)").show()

  }

  @Test
  def testExist(): Unit ={

  }

  @After
  def close(): Unit ={
    db.close()
  }
}
