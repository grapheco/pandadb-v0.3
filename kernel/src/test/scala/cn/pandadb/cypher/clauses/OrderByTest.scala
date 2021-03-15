package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.LynxString
import org.junit.{AfterClass, Assert, BeforeClass, Test}


object OrderByTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"A", "length"->170, "age"->34, "no"->13))
    val n2 = db.addNode(Map("name" -> "B", "age" -> 34, "no"->14) )
    val n3 = db.addNode(Map("name" -> "C", "length"->185, "age" -> 32, "no"->1) )
    val r1 = db.addRelation("KNOWS", n1, n2, Map())
    val r2 = db.addRelation("KNOWS", n2, n3, Map())
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}

class OrderByTest {
  val db = OrderByTest.db

  @Test
  def test1(): Unit = {
    // Order nodes by property
    val cypher = "MATCH (n) RETURN n.name, n.age ORDER BY n.no"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(3, res.length)
    val names = res.map(_.get("n.name").get).toArray
    println(names.toList)
    Assert.assertEquals("C", names(0).asInstanceOf[LynxString].value)
    Assert.assertEquals("A", names(1).asInstanceOf[LynxString].value)
    Assert.assertEquals("B", names(2).asInstanceOf[LynxString].value)
  }

  @Test
  def test2(): Unit = {
    // Order nodes by multiple properties
    val cypher = "MATCH (n) RETURN n.name, n.age ORDER BY n.age, n.name"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(3, res.length)
    val names = res.map(_.get("n.name").get).toArray
    Assert.assertEquals("C", names(0).asInstanceOf[LynxString].value)
    Assert.assertEquals("A", names(1).asInstanceOf[LynxString].value)
    Assert.assertEquals("B", names(2).asInstanceOf[LynxString].value)
  }

  @Test
  def test3(): Unit = {
    // Order nodes by multiple properties
    val cypher = "MATCH (n) RETURN n.name, n.age ORDER BY n.name DESC"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(3, res.length)
    val names = res.map(_.get("n.name").get).toArray
    Assert.assertEquals("C", names(0).asInstanceOf[LynxString].value)
    Assert.assertEquals("B", names(1).asInstanceOf[LynxString].value)
    Assert.assertEquals("A", names(2).asInstanceOf[LynxString].value)
  }

  @Test
  def test4(): Unit = {
    // Ordering null
    val cypher = "MATCH (n) RETURN n.length, n.name, n.age ORDER BY n.length"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(3, res.length)
    val names = res.map(_.get("n.name").get).toArray
    Assert.assertEquals("A", names(0).asInstanceOf[LynxString].value)
    Assert.assertEquals("C", names(1).asInstanceOf[LynxString].value)
    Assert.assertEquals("B", names(2).asInstanceOf[LynxString].value)
  }

  @Test
  def test5(): Unit = {
    // Ordering in a WITH clause
    val cypher = "MATCH (n) WITH n ORDER BY n.age RETURN collect(n.name) AS names"
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    val names = res(0).get("names").get.asInstanceOf[Iterable[String]].toArray
    Assert.assertEquals("A", names(0).asInstanceOf[LynxString].value)
    Assert.assertEquals("C", names(1).asInstanceOf[LynxString].value)
    Assert.assertEquals("B", names(2).asInstanceOf[LynxString].value)
  }

}
