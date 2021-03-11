package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}


object UnwindTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}

class UnwindTest {
  val db = UnwindTest.db

  @Test
  def test1(): Unit = {
    // Unwinding a list
    val cypher = "UNWIND [1, 2, 3, null] AS x RETURN x, 'val' AS y"
    val res = db.cypher(cypher).records()
    Assert.assertEquals(4, res.length)
  }

  @Test
  def test2(): Unit = {
    // Creating a distinct list
    val cypher = """WITH [1, 1, 2, 2] AS coll
                   |UNWIND coll AS x
                   |WITH DISTINCT x
                   |RETURN collect(x) AS setOfVals""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def test3(): Unit = {
    // Using UNWIND with any expression returning a list
    val cypher = """WITH
                   |  [1, 2] AS a,
                   |  [3, 4] AS b
                   |UNWIND (a + b) AS x
                   |RETURN x""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(4, res.length)
  }

  @Test
  def test4(): Unit = {
    // Using UNWIND with a list of lists
    val cypher = """WITH [[1, 2], [3, 4], 5] AS nested
                   |UNWIND nested AS x
                   |UNWIND x AS y
                   |RETURN y""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(5, res.length)
  }

  @Test
  def test5(): Unit = {
    // Using UNWIND with an empty list
    val cypher = """UNWIND [] AS empty
                   |RETURN empty, 'literal_that_is_not_returned' """.stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(0, res.length)

    // To avoid inadvertently using UNWIND on an empty list, CASE may be used to replace an empty list with a null:
    val cypher2 = """WITH [] AS list
                    |UNWIND
                    |  CASE
                    |    WHEN list = [] THEN [null]
                    |    ELSE list
                    |  END AS emptylist
                    |RETURN emptylist""".stripMargin
    val res2 = db.cypher(cypher2).records()
    Assert.assertEquals(0, res2.length)
  }

  @Test
  def test6(): Unit = {
    // Using UNWIND with an expression that is not a list
    val cypher = """UNWIND null AS x
                   |RETURN x, 'some_literal'""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(0, res.length)
  }

  @Test
  def test7(): Unit = {
    // Creating nodes from a list parameter
    val cypher1 = """:params {
                   |  "events" : [ {
                   |    "year" : 2014,
                   |    "id" : 1
                   |  }, {
                   |    "year" : 2014,
                   |    "id" : 2
                   |  } ]
                   |}""".stripMargin.replace("\r\n", " ")
    db.cypher(cypher1).show()

    val cypher2 = """UNWIND $events AS event
                    |MERGE (y:Year { year: event.year })
                    |MERGE (y)<-[:IN]-(e:Event { id: event.id })
                    |RETURN e.id AS x
                    |ORDER BY x""".stripMargin
    val res = db.cypher(cypher2).records()
    Assert.assertEquals(2, res.length)
  }



}
