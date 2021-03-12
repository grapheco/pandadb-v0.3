package cn.pandadb.cypher.functions

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}


object MathematicalFuntionsNumericTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"Alice", "eyes"->"brown", "age"->38), "A")
    val n2 = db.addNode(Map("name"->"Charlie", "eyes"->"green", "age"->53), "C")
    val n3 = db.addNode(Map("name"->"Bob", "eyes"->"blue", "age"->25), "B")
    val n4 = db.addNode(Map("name"->"Daniel", "eyes"->"brown", "age"->54), "D")
    val n5 = db.addNode(Map("name"->"Eskil", "eyes"->"blue", "age"->41, "array"->Array[String]("one","two","three")), "E")

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

class MathematicalFuntionsNumericTest {
  val db = MathematicalFuntionsNumericTest.db

  @Test
  def absTest(): Unit = {
    // abs() returns the absolute value of the given number.
    //Syntax: abs(expression)
    val cypher =
      """MATCH (a), (e) WHERE a.name = 'Alice' AND e.name = 'Eskil'
        |RETURN a.age, e.age, abs(a.age - e.age)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3, res(0).get("abs(a.age - e.age)").get)
  }

  @Test
  def ceilTest(): Unit = {
    // ceil() returns the smallest floating point number that is greater than or equal to the given number
    // and equal to a mathematical integer.
    //Syntax: ceil(expression)
    val cypher = """RETURN ceil(0.1)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(1.0, res(0).get("ceil(0.1)").get)
  }

  @Test
  def floorTest(): Unit = {
    // floor() returns the largest floating point number that is less than or equal to the given number
    // and equal to a mathematical integer.
    //Syntax: floor(expression)
    val cypher = """RETURN floor(0.9)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(0.0, res(0).get("floor(0.9)").get)
  }

  @Test
  def randTest(): Unit = {
    // rand() returns a random floating point number in the range from 0 (inclusive) to 1 (exclusive);
    // i.e. [0,1). The numbers returned follow an approximate uniform distribution.
    //Syntax: rand()
    val cypher = """RETURN rand()""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def roundTest1(): Unit = {
    // round() returns the value of the given number rounded to the nearest integer, with half-way values always rounded up.
    //Syntax: round(expression)
    val cypher = """RETURN round(3.141592)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3.0, res(0).get("round(3.141592)").get)
  }

  @Test
  def roundTest2(): Unit = {
    // round() returns the value of the given number rounded with the specified precision, with half-values always being rounded up.
    //Syntax: round(expression, precision)
    val cypher = """RETURN round(3.141592, 3)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3.142, res(0).get("round(3.141592, 3)").get)
  }

  @Test
  def roundTest3(): Unit = {
    // round() returns the value of the given number rounded with the specified precision and the specified rounding mode.
    //Syntax: round(expression, precision, mode)
    val cypher = """RETURN round(3.141592, 2, 'CEILING')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3.15, res(0).get("round(3.141592, 2, 'CEILING')").get)
  }

  @Test
  def signTest3(): Unit = {
    // sign() returns the signum of the given number: 0 if the number is 0, -1 for any negative number, and 1 for any positive number.
    //Syntax: sign(expression)
    val cypher = """RETURN sign(-17), sign(0.1)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(-1, res(0).get("sign(-17)").get)
    Assert.assertEquals(1, res(0).get("sign(0.1)").get)
  }
}
