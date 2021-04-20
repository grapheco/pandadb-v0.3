package cn.pandadb.cypher.functions

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}


object PredicateFunctionsTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"Alice", "eyes"->"brown", "age"->38))
    val n2 = db.addNode(Map("name"->"Charlie", "eyes"->"green", "age"->53))
    val n3 = db.addNode(Map("name"->"Bob", "eyes"->"blue", "age"->25))
    val n4 = db.addNode(Map("name"->"Daniel", "eyes"->"brown", "age"->54))
    val n5 = db.addNode(Map("name"->"Eskil", "eyes"->"blue", "age"->41, "array"->Array[String]("one","two","three")))
    val n6 = db.addNode(Map("eyes"->"brown", "age"->61))

    val r1 = db.addRelation("KNOWS", n1, n2, Map())
    val r2 = db.addRelation("KNOWS", n1, n3, Map())
    val r3 = db.addRelation("KNOWS", n2, n4, Map())
    val r4 = db.addRelation("KNOWS", n3, n4, Map())
    val r5 = db.addRelation("MARRIED", n3, n5, Map())
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}

class PredicateFunctionsTest {
  val db = PredicateFunctionsTest.db

  @Test
  def allTest(): Unit = {
    // Syntax: all(variable IN list WHERE predicate)
    val cypher = """MATCH p = (a)-[*1..3]->(b)
                   |WHERE a.name = 'Alice' AND b.name = 'Daniel'
                   |AND all(x IN nodes(p) WHERE x.age > 30)
                   |RETURN p""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def anyTest(): Unit = {
    // Syntax: any(variable IN list WHERE predicate)
    val cypher = """MATCH (a)
                   |WHERE a.name = 'Eskil'
                   |AND any(x IN a.array WHERE x = 'one')
                   |RETURN a.name, a.array""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def existsTest1(): Unit = {
    // Syntax: exists(pattern-or-property)
    val cypher = """MATCH (n)
                   |WHERE exists(n.name)
                   |RETURN n.name AS name, exists((n)-[:MARRIED]->()) AS is_married""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(5, res.length)
  }

  @Test
  def existsTest2(): Unit = {
    // Syntax: exists(pattern-or-property)
    val cypher = """MATCH (a), (b)
                   |WHERE exists(a.name) AND NOT exists(b.name)
                   |OPTIONAL MATCH (c:DoesNotExist)
                   |RETURN a.name AS a_name, b.name AS b_name, exists(b.name) AS b_has_name, c.name AS c_name, exists(c.name) AS c_has_name
                   |ORDER BY a_name, b_name, c_name LIMIT 1""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def noneTest(): Unit = {
    // Syntax: none(variable IN list WHERE predicate)
    val cypher = """MATCH p = (n)-[*1..3]->(b)
                   |WHERE n.name = 'Alice'
                   |AND none(x IN nodes(p) WHERE x.age = 25) RETURN p""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(2, res.length)
  }

  @Test
  def singleTest(): Unit = {
    // Syntax: single(variable IN list WHERE predicate)
    val cypher = """MATCH p = (n)-->(b)
                   |WHERE n.name = 'Alice'
                   |AND single(var IN nodes(p) WHERE var.eyes = 'blue')
                   |RETURN p""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

}
