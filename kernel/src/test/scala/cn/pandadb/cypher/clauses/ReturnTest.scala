package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}

object ReturnTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("happy"->"Yes!", "name"->"A", "age"->55))
    val n2 = db.addNode(Map("name" -> "B"))
    val r1 = db.addRelation("BLOCKS", n1, n2, Map())
    val r2 = db.addRelation("KNOWS", n1, n2, Map())
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}

class ReturnTest {
  val db = ReturnTest.db

  @Test
  def test1(): Unit = {
    // Return nodes
    val cypher = "MATCH (n {name: 'B'}) RETURN n"
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def test2(): Unit = {
    // Return relationships
    val cypher = "MATCH (n {name: 'A'})-[r:KNOWS]->(c) RETURN r"
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def test3(): Unit = {
    // Return property
    val cypher = "MATCH (n {name: 'A'}) RETURN n.name"
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def test4(): Unit = {
    // Return all elements
    val cypher = "MATCH p = (a {name: 'A'})-[r]->(b) RETURN *"
    val res = db.cypher(cypher)
    Assert.assertEquals(2, res.records().length)
    Assert.assertEquals(4, res.columns().length)
  }

  @Test
  def test5(): Unit = {
    // Variable with uncommon characters
    val cypher = """MATCH (`This isn\'t a common variable`)
                   |WHERE `This isn\'t a common variable`.name = 'A'
                   |RETURN `This isn\'t a common variable`.happy""".stripMargin
    val res = db.cypher(cypher)
    Assert.assertEquals(1, res.records().length)
  }

  @Test
  def test6(): Unit = {
    // Column alias
    val cypher = "MATCH (a {name: 'A'}) RETURN a.age AS SomethingTotallyDifferent"
    val res = db.cypher(cypher)
    Assert.assertEquals(1, res.records().length)
  }

  @Test
  def test7(): Unit = {
    // Optional properties
    val cypher = "MATCH (n) RETURN n.age"
    val res = db.cypher(cypher)
    Assert.assertEquals(2, res.records().length)
  }

  @Test
  def test8(): Unit = {
    // Other expressions
    val cypher = "MATCH (a {name: 'A'}) RETURN a.age > 30, \"I'm a literal\", (a)-->()"
    val res = db.cypher(cypher)
    Assert.assertEquals(1, res.records().length)
  }

  @Test
  def test9(): Unit = {
    // Other expressions
    val cypher = "MATCH (a {name: 'A'})-->(b) RETURN DISTINCT b"
    val res = db.cypher(cypher)
    Assert.assertEquals(1, res.records().length)
  }


}
