package cn.pandadb.cypher.functions

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}



object ScalarFunctionsTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"Alice", "eyes"->"brown", "age"->38), "Developer")
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

class ScalarFunctionsTest {
  val db = ScalarFunctionsTest.db

  @Test
  def coalesceTest(): Unit = {
    // coalesce() returns the first non-null value in the given list of expressions.
    // Syntax: coalesce(expression [, expression]*)
    val cypher = """MATCH (a)
                   |WHERE a.name = 'Alice'
                   |RETURN coalesce(a.hairColor, a.eyes)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals("brown", res.next().get("coalesce(a.hairColor, a.eyes)").get)
  }

  @Test
  def endNodeTest(): Unit = {
    // endNode() returns the end node of a relationship.
    //Syntax: endNode(relationship)
    val cypher = """MATCH (x:Developer)-[r]-()
                   |RETURN endNode(r)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(2, res.length)
  }

  @Test
  def headTest(): Unit = {
    // head() returns the first element in a list.
    //Syntax: head(list)
    val cypher = """MATCH (a)
                   |WHERE a.name = 'Eskil'
                   |RETURN a.array, head(a.array)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals("one", res.next().get("head(a.array)").get)
  }

  @Test
  def idTest(): Unit = {
    // id() returns the id of a relationship or node.
    //Syntax: id(expression)
    val cypher = """MATCH (a)
                   |RETURN id(a)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(5, res.length)
  }

  @Test
  def lastTest(): Unit = {
    // last() returns the last element in a list.
    //Syntax: last(expression)
    val cypher = """MATCH (a)
                   |WHERE a.name = 'Eskil'
                   |RETURN a.array, last(a.array)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("three", res(0).get("last(a.array)").get)
  }

  @Test
  def lengthTest(): Unit = {
    // length() returns the length of a path.
    //Syntax: length(path)
    val cypher = """MATCH p = (a)-->(b)-->(c)
                   |WHERE a.name = 'Alice'
                   |RETURN length(p)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(3, res.length)
    res.foreach(m => Assert.assertEquals(2, m.get("length(p)").get))
  }

  @Test
  def propertiesTest(): Unit = {
    // properties() returns a map containing all the properties of a node or relationship.
    // If the argument is already a map, it is returned unchanged.
    //Syntax: properties(expression)
    val cypher = """CREATE (p:Person {name: 'Stefan', city: 'Berlin'})
                   |RETURN properties(p)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def randomUUIDTest(): Unit = {
    // randomUUID() returns a randomly-generated Universally Unique Identifier (UUID),
    // also known as a Globally Unique Identifier (GUID). This is a 128-bit value with strong guarantees of uniqueness.
    //Syntax: randomUUID()
    val cypher = """RETURN randomUUID() AS uuid""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def sizeTest(): Unit = {
    // size() returns the number of elements in a list.
    //Syntax: size(list)
    val cypher = """RETURN size(['Alice', 'Bob'])""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(2, res(0).get("size(['Alice', 'Bob'])").get)
  }

  @Test
  def sizeTest2(): Unit = {
    // size() applied to pattern expression
    //Syntax: size(pattern expression)
    val cypher = """MATCH (a)
                   |WHERE a.name = 'Alice'
                   |RETURN size((a)-->()-->()) AS fof""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3, res(0).get("fof").get)
  }

  @Test
  def sizeTest3(): Unit = {
    // size() applied to string
    //Syntax: size(string)
    val cypher = """MATCH (a)
                   |WHERE size(a.name) > 6
                   |RETURN size(a.name)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(7, res(0).get("size(a.name)").get)
  }

  @Test
  def startNodeTest(): Unit = {
    // startNode() returns the start node of a relationship.
    //Syntax: startNode(relationship)
    val cypher = """MATCH (x:Developer)-[r]-()
                   |RETURN startNode(r)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(2, res.length)
  }

  @Test
  def timestampTest(): Unit = {
    // timestamp() returns the difference, measured in milliseconds,
    // between the current time and midnight, January 1, 1970 UTC. It is the equivalent of datetime().epochMillis.
    //Syntax: timestamp()
    val cypher = """RETURN timestamp()""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def toBooleanTest(): Unit = {
    // toBoolean() converts a string value to a boolean value.
    //Syntax: toBoolean(expression)
    val cypher = """RETURN toBoolean('TRUE'), toBoolean('not a boolean')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(true, res(0).get("toBoolean('TRUE')").get)
    Assert.assertEquals(false, res(0).get("toBoolean('not a boolean')").get)
  }

  @Test
  def toFloatTest(): Unit = {
    // toFloat() converts an integer or string value to a floating point number.
    //Syntax: toFloat(expression)
    val cypher = """RETURN toFloat('11.5'), toFloat('not a number')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(11.5, res(0).get("toFloat('11.5')").get)
    Assert.assertEquals(null, res(0).get("toFloat('not a number')").get)
  }

  @Test
  def toIntegerTest(): Unit = {
    // toInteger() converts a floating point or string value to an integer value.
    //Syntax: toInteger(expression)
    val cypher = """RETURN toInteger('42'), toInteger('not a number')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(42, res(0).get("toInteger('42')").get)
    Assert.assertEquals(null, res(0).get("toInteger('not a number')").get)
  }

  @Test
  def typeTest(): Unit = {
    // type() returns the string representation of the relationship type.
    //Syntax: type(relationship)
    val cypher = """MATCH (n)-[r]->()
                   |WHERE n.name = 'Alice'
                   |RETURN type(r)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(2, res.length)
    res.foreach(m=>Assert.assertEquals("KNOWS",m.get("type(r)").get))
  }
}
