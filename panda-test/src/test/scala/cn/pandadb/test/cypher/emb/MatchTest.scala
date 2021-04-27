package cn.pandadb.test.cypher.emb

import java.io.File

import cn.pandadb.kernel.{GraphDatabaseBuilder, GraphService}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxNode, LynxValue}
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: supported cypher clauses
 * @author: LiamGao
 * @create: 2021-04-26
 */
class MatchTest {
  val dbPath = "./testdata/emb"
  var db: GraphService = _

  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File(dbPath))
    FileUtils.forceMkdir(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath)

    val id1 = db.addNode(Map("name"->"alex",
      "storage"->1000000,
      "salary"->2333.33,
      "isCoder"->true,
      "indexes"->Array[Int](1,2,3,4,5),
      "floats"->Array[Double](11.1, 22.2, 33.3),
      "bools"->Array[Boolean](true, true, false, false),
      "jobs"->Array[String]("teacher", "coder", "singer")), "person")

    val id2 = db.addNode(Map("name"->"bob",
      "storage"->1000000,
      "indexes"->Array[Int](1,2,2,2,2)), "people")

    val id3 = db.addNode(Map("name"->"clause",
      "storage"->66666,
      "isCoder"->true,
      "indexes"->Array[Int](1,2,3,4,5)), "person")
    db.addRelation("TMP", id1, id2, Map())
    db.addRelation("TMP", id2, id3, Map("index"->2))

    val n1 = db.addNode(Map("name"->"Oliver Stone", "sex"->"male"), "Person","Director")
    val n2 = db.addNode(Map("name"->"Michael Douglas"), "Person")
    val n3 = db.addNode(Map("name"->"Charlie Sheen"), "Person")
    val n4 = db.addNode(Map("name"->"Martin Sheen"), "Person")
    val n5 = db.addNode(Map("name"->"Rob Reiner"), "Person", "Director")
    val m1 = db.addNode(Map("title"->"Wall Street", "year"->1987), "Movie")
    val m2 = db.addNode(Map("title"->"The American President"), "Movie")

    val directedR1 = db.addRelation("DIRECTED", n1, m1, Map())
    val directedR2 = db.addRelation("DIRECTED", n5, m2, Map())
    val actedR1 = db.addRelation("ACTED_IN", n2, m1, Map("role"->"Gordon Gekko"))
    val actedR2 = db.addRelation("ACTED_IN", n2, m2, Map("role"->"President Andrew Shepherd"))
    val actedR3 = db.addRelation("ACTED_IN", n3, m1, Map("role"->"Bud Fox"))
    val actedR4 = db.addRelation("ACTED_IN", n4, m1, Map("role"->"Carl Fox"))
    val actedR5 = db.addRelation("ACTED_IN", n4, m2, Map("role"->"A.J. MacInerney"))
  }

  @Test
  def testMatchNodeWithLabelOrNot(): Unit ={
    val res1 = db.cypher("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(10L, res1)

    val res2 = db.cypher("match (n:person) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(2L, res2)

    val res3 = db.cypher("match (n:people) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(1L, res3)
  }

  @Test
  def testMatchNodeWithLabelAndProperty(): Unit ={
    val res1 = db.cypher("match (n) where n.storage=1000000 return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(2L, res1)

    val res2 = db.cypher("match (n) where n.indexes=[1,2,2,2,2] return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals("bob", res2.property("name").get.value)

    val res3 = db.cypher("match (n) where n.indexes=[1,2,3,4,5] and n.storage=66666 return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals("clause", res3.property("name").get.value)

    val res4 = db.cypher("match (n) where n.storage=1000001 return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(0L, res4)

    val res5 = db.cypher("match (n:person) where n.storage=1000000 return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(1L, res5)

    val res6 = db.cypher("match (n:Movie) where n.title='Wall Street' return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(1L, res6)
  }

  @Test
  def testMatchRelationships(): Unit ={
    val res1 = db.cypher("match (n)-[r]->(m) return count(r)").records().next()("count(r)").asInstanceOf[LynxValue].value
    Assert.assertEquals(9L, res1)

    val res2 = db.cypher("match (n)-[r]-(m) return count(r)").records().next()("count(r)").asInstanceOf[LynxValue].value
    Assert.assertEquals(18L, res2)

    val res3 = db.cypher("match (n:Person)-[r]->(m) return count(r)").records().next()("count(r)").asInstanceOf[LynxValue].value
    Assert.assertEquals(7L, res3)

    val res4 = db.cypher("match (n)-[r]->(m:people) return count(r)").records().next()("count(r)").asInstanceOf[LynxValue].value
    Assert.assertEquals(1L, res4)

    val res5 = db.cypher("match (n)-[r:DIRECTED]->(m) return count(r)").records().next()("count(r)").asInstanceOf[LynxValue].value
    Assert.assertEquals(2L, res5)

    val res6 = db.cypher("match (n)-[r:ACTED_IN]->(m) return count(r)").records().next()("count(r)").asInstanceOf[LynxValue].value
    Assert.assertEquals(5L, res6)

    val res7 = db.cypher("match data = (n)-[r1]->(m)-[r2]->(q) return count(data)").records().next()("count(data)").asInstanceOf[LynxValue].value
    Assert.assertEquals(1L, res7)
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
