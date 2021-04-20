package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}

object WhereTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"Andy", "belt"->"white", "age"->36), "Swedish")
    val n2 = db.addNode(Map("name"->"Peter", "email"->"peter_n@example.com", "age"->35))
    val n3 = db.addNode(Map("name"->"Timothy", "address"->"Sweden/Malmo", "age"->25))

    val r1 = db.addRelation("KNOWS", n1, n2, Map("since"->1999))
    val r2 = db.addRelation("BLOCKS", n1, n3, Map("since"->2012))
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}


class WhereTest {
  val db = WhereTest.db

  @Test
  def basicUsageTest1(): Unit = {
    // Boolean operations
    val cypher = """MATCH (n)
                   |WHERE n.name = 'Peter' XOR (n.age < 30 AND n.name = 'Timothy') OR NOT (n.name = 'Timothy' OR n.name = 'Peter')
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(3, res.length)
  }

  @Test
  def basicUsageTest2(): Unit = {
    // Filter on node label
    val cypher = """MATCH (n)
                    |WHERE n:Swedish
                    |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def basicUsageTest3(): Unit = {
    // Filter on node property
    val cypher = """MATCH (n)
                   |WHERE n.age < 30
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def basicUsageTest4(): Unit = {
    // Filter on relationship property
    val cypher = """MATCH (n)-[k:KNOWS]->(f)
                   |WHERE k.since < 2000
                   |RETURN f.name, f.age, f.email""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def basicUsageTest5(): Unit = {
    // Filter on dynamically-computed node property
    val cypher = """WITH 'AGE' AS propname
                   |MATCH (n)
                   |WHERE n[toLower(propname)]< 30
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def basicUsageTest6(): Unit = {
    // Property existence checking
    val cypher = """MATCH (n)
                   |WHERE exists(n.belt)
                   |RETURN n.name, n.belt""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def stringMatchTest1(): Unit = {
    // Prefix string search using STARTS WITH
    val cypher = """MATCH (n)
                   |WHERE n.name STARTS WITH 'Pet'
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def stringMatchTest2(): Unit = {
    // Suffix string search using ENDS WITH
    val cypher = """MATCH (n)
                   |WHERE n.name ENDS WITH 'ter'
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def stringMatchTest3(): Unit = {
    // Substring search using CONTAINS
    val cypher = """MATCH (n)
                   |WHERE n.name CONTAINS 'ete'
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def stringMatchTest4(): Unit = {
    // String matching negation
    val cypher = """MATCH (n)
                   |WHERE NOT n.name ENDS WITH 'y'
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def regularExpressionsTest1(): Unit = {
    // Matching using regular expressions
    val cypher = """MATCH (n)
                   |WHERE n.name =~ 'Tim.*'
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def regularExpressionsTest2(): Unit = {
    // Escaping in regular expressions
    val cypher = """MATCH (n)
                   |WHERE n.email =~ '.*\\.com'
                   |RETURN n.name, n.age, n.email""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def regularExpressionsTest3(): Unit = {
    // Case-insensitive regular expressions
    val cypher = """MATCH (n)
                   |WHERE n.name =~ '(?i)AND.*'
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def pathPatternsTest1(): Unit = {
    // Filter on patterns
    val cypher = """MATCH (timothy { name: 'Timothy' }),(others)
                   |WHERE others.name IN ['Andy', 'Peter'] AND (timothy)<--(others)
                   |RETURN others.name, others.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def pathPatternsTest2(): Unit = {
    // Filter on patterns using NOT
    val cypher = """MATCH (persons),(peter { name: 'Peter' })
                   |WHERE NOT (persons)-->(peter)
                   |RETURN persons.name, persons.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(2, res.length)
  }

  @Test
  def pathPatternsTest3(): Unit = {
    // Filter on patterns with properties
    val cypher = """MATCH (n)
                   |WHERE (n)-[:KNOWS]-({ name: 'Timothy' })
                   |RETURN n.name, n.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def pathPatternsTest4(): Unit = {
    // Filter on relationship type
    val cypher = """MATCH (n)-[r]->()
                   |WHERE n.name='Andy' AND type(r)=~ 'K.*'
                   |RETURN type(r), r.since""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(2, res.length)
  }

  @Test
  def listsTest1(): Unit = {
    // IN operator
    val cypher = """MATCH (a)
                   |WHERE a.name IN ['Peter', 'Timothy']
                   |RETURN a.name, a.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(2, res.length)
  }

  @Test
  def missingPropertiesValuesTest1(): Unit = {
    // Default to false if property is missing
    val cypher = """MATCH (n)
                   |WHERE n.belt = 'white'
                   |RETURN n.name, n.age, n.belt""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def missingPropertiesValuesTest2(): Unit = {
    // Default to true if property is missing
    val cypher = """MATCH (n)
                   |WHERE n.belt = 'white' OR n.belt IS NULL RETURN n.name, n.age, n.belt
                   |ORDER BY n.name""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(3, res.length)
  }

  @Test
  def missingPropertiesValuesTest3(): Unit = {
    // Filter on null
    val cypher = """MATCH (person)
                   |WHERE person.name = 'Peter' AND person.belt IS NULL
                   |RETURN person.name, person.age, person.belt""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def rangesTest1(): Unit = {
    // Simple range
    val cypher = """MATCH (a)
                   |WHERE a.name >= 'Peter'
                   |RETURN a.name, a.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(2, res.length)
  }

  @Test
  def rangesTest2(): Unit = {
    // Composite  range
    val cypher = """MATCH (a)
                   |WHERE a.name > 'Andy' AND a.name < 'Timothy'
                   |RETURN a.name, a.age""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }
}
