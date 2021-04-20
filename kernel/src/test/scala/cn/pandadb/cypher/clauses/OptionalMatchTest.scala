package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}

object OptionalMatchTest{
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"Oliver Stone"), "Person")
    val n2 = db.addNode(Map("name"->"Michael Douglas"), "Person")
    val n3 = db.addNode(Map("name"->"Charlie Sheen"), "Person")
    val n4 = db.addNode(Map("name"->"Martin Sheen"), "Person")
    val n5 = db.addNode(Map("name"->"Rob Reiner"), "Person", "Director")
    val m1 = db.addNode(Map("title"->"Wall Street"), "Movie")
    val m2 = db.addNode(Map("title"->"The American President"), "Movie")

    val directedR1 = db.addRelation("DIRECTED", n1, m1, Map())
    val directedR2 = db.addRelation("DIRECTED", n5, m2, Map())
    val actedR1 = db.addRelation("ACTED_IN", n2, m1, Map("role"->"Gordon Gekko"))
    val actedR2 = db.addRelation("ACTED_IN", n2, m2, Map("role"->"President Andrew Shepherd"))
    val actedR3 = db.addRelation("ACTED_IN", n3, m1, Map("role"->"Bud Fox"))
    val fatherR6 = db.addRelation("FATHER", n3, n4, Map())
    val actedR4 = db.addRelation("ACTED_IN", n4, m1, Map("role"->"Carl Fox"))
    val actedR5 = db.addRelation("ACTED_IN", n4, m2, Map("role"->"A.J. MacInerney"))
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}


class OptionalMatchTest {
  val db = OptionalMatchTest.db

  @Test
  def test1(): Unit = {
    // Optional relationships
    val cypher = "MATCH (a:Movie {title: 'Wall Street'}) OPTIONAL MATCH (a)-->(x) RETURN x"
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def test2(): Unit = {
    // Properties on optional elements
    val cypher = "MATCH (a:Movie {title: 'Wall Street'}) OPTIONAL MATCH (a)-->(x) RETURN x, x.name"
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def test3(): Unit = {
    // Properties on optional elements
    val cypher = "MATCH (a:Movie {title: 'Wall Street'}) OPTIONAL MATCH (a)-[r:ACTS_IN]->() RETURN a.title, r"
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }



}
