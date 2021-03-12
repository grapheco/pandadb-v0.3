package cn.pandadb.cypher.administration.schema

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.store.NodeStoreSPI
import org.apache.commons.io.FileUtils
import org.junit.{After, AfterClass, Assert, Before, BeforeClass, Test}



class IndexesTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @Before
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"Alice", "eyes"->"brown", "age"->38), "Person", "Developer")
    val n2 = db.addNode(Map("name"->"Charlie", "eyes"->"green", "age"->53), "Person")
    val n3 = db.addNode(Map("name"->"Bob", "eyes"->"blue", "age"->25), "Person")
    val n4 = db.addNode(Map("name"->"Daniel", "eyes"->"brown", "age"->54), "Person")
    val n5 = db.addNode(Map("name"->"Eskil", "eyes"->"blue", "age"->41, "array"->Array[String]("one","two","three")), "Person")

    val r1 = db.addRelation("KNOWS", n1, n2, Map())
    val r2 = db.addRelation("KNOWS", n1, n3, Map())
    val r3 = db.addRelation("KNOWS", n2, n4, Map())
    val r4 = db.addRelation("KNOWS", n3, n4, Map())
    val r5 = db.addRelation("MARRIED", n3, n5, Map())
  }

  @After
  def closeDB():Unit = {
    db.close()
  }


  @Test
  def createSinglePropertyIndexTest(): Unit = {
    // Create a single-property index
    val cypher = """CREATE INDEX ON :Person(name)""".stripMargin
    db.cypher(cypher).show()
    Assert.assertTrue(db.hasIndex(Set("Person"), Set("name")).isDefined)
  }

  @Test
  def createCompositeIndexTest(): Unit = {
    // Create a composite index
    val cypher = """CREATE INDEX ON :Person(age, eyes)""".stripMargin
    db.cypher(cypher).show()
    Assert.assertTrue(db.hasIndex(Set("Person"), Set("age","eyes")).isDefined)
  }

  @Test
  def getIndexesTest(): Unit = {
    // Get a list of all indexes in the database
    db.createIndexOnNode("Person", Set("name"))
    db.createIndexOnNode("Person", Set("name", "age"))
    val cypher = """CALL db.indexes""".stripMargin
    val res = db.cypher(cypher).records().toArray
    println(res)
    Assert.assertTrue(res.length > 0)
  }

  @Test
  def dropSinglePropertyIndexTest(): Unit = {
    // Drop a single-property index
    db.createIndexOnNode("Person", Set("name"))
    db.createIndexOnNode("Person", Set("name", "age"))
    val cypher = """DROP INDEX ON :Person(name)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertTrue(db.hasIndex(Set("Person"), Set("name")).isEmpty)
  }

  @Test
  def dropCompositeIndexTest(): Unit = {
    // Drop a composite index
    db.createIndexOnNode("Person", Set("name"))
    db.createIndexOnNode("Person", Set("name", "age"))
    val cypher = """DROP INDEX ON :Person(name,age)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertTrue(db.hasIndex(Set("Person"), Set("name", "age")).isEmpty)

  }

}
