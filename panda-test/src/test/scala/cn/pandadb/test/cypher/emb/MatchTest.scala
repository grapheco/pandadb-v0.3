package cn.pandadb.test.cypher.emb

import java.io.File

import cn.pandadb.kernel.{GraphDatabaseBuilder, GraphService}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.LynxValue
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: supported cypher clauses
 * @author: LiamGao
 * @create: 2021-04-26
 */
class MatchTest {
  val dbPath = "./testdata/emb"
  var db: GraphService = _

  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File(dbPath))
    FileUtils.forceMkdir(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath)
  }


  @Test
  def testMatch(): Unit ={
    val id1 = db.addNode(Map("name"->"alex",
      "storage"->1000000,
      "salary"->2333.33,
      "isCoder"->true,
      "indexs"->Array[Int](1,2,3,4,5),
      "floats"->Array[Double](11.1, 22.2, 33.3),
      "bools"->Array[Boolean](true, true, false, false),
      "jobs"->Array[String]("teacher", "coder", "singer")), "person")

    val id2 = db.addNode(Map("name"->"bob",
      "storage"->1000000,
      "indexs"->Array[Int](1,2,3,4,5)), "people")

    val id3 = db.addNode(Map("name"->"clause",
      "storage"->1000000,
      "isCoder"->true,
      "indexs"->Array[Int](1,2,3,4,5)), "person")

    val res = db.cypher("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(3, res)
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
