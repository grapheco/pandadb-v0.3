package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}


object SkipAndLimitTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"A"))
    val n2 = db.addNode(Map("name"->"E"))
    val n3 = db.addNode(Map("name"->"D"))
    val n4 = db.addNode(Map("name"->"C"))
    val n5 = db.addNode(Map("name"->"B"))
    val r1 = db.addRelation("KNOWS", n1, n2, Map())
    val r2 = db.addRelation("KNOWS", n1, n3, Map())
    val r3 = db.addRelation("KNOWS", n1, n4, Map())
    val r4 = db.addRelation("KNOWS", n1, n5, Map())
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}

class SkipAndLimitTest {
  val db = SkipAndLimitTest.db

  @Test
  def skipTest1(): Unit = {
    //  Skip first three rows
    val cypher = "MATCH (n) RETURN n.name ORDER BY n.name SKIP 3"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(2, res.length)
    val names = res.map(_.get("n.name").get).toArray
    Assert.assertEquals("D", names(0))
    Assert.assertEquals("E", names(1))
  }

  @Test
  def skipTest2(): Unit = {
    // Return middle two rows
    val cypher = "MATCH (n) RETURN n.name ORDER BY n.name SKIP 1 LIMIT 2"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(2, res.length)
    val names = res.map(_.get("n.name").get).toArray
    Assert.assertEquals("B", names(0))
    Assert.assertEquals("C", names(1))
  }

  @Test
  def skipTest3(): Unit = {
    // Using an expression with SKIP to return a subset of the rows
    val cypher = "MATCH (n) RETURN n.name ORDER BY n.name SKIP 1 + toInteger(3*0.5)"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(3, res.length)
    val names = res.map(_.get("n.name").get).toArray
    Assert.assertEquals("C", names(0))
    Assert.assertEquals("D", names(1))
    Assert.assertEquals("E", names(2))
  }

  @Test
  def limitTest1(): Unit = {
    // Return a limited subset of the rows
    val cypher = "MATCH (n) RETURN n.name ORDER BY n.name LIMIT 3"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(3, res.length)
    val names = res.map(_.get("n.name").get).toArray
    Assert.assertEquals("A", names(0))
    Assert.assertEquals("B", names(1))
    Assert.assertEquals("C", names(2))
  }

  @Test
  def limitTest2(): Unit = {
    // Using an expression with LIMIT to return a subset of the rows
    val cypher = "MATCH (n) RETURN n.name ORDER BY n.name LIMIT 1 + toInteger(3 * rand())"
    val res = db.cypher(cypher).records().toArray
    Assert.assertTrue( res.length>=1)
    val names = res.map(_.get("n.name").get).toArray
    Assert.assertEquals("A", names(0))
  }

}
