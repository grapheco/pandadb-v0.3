package cn.pandadb.test.cypher.emb

import java.io.File

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxNode, LynxRelationship, LynxValue}
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: supported cypher clauses
 * @author: LiamGao
 * @create: 2021-04-26
 */
class MatchTest {
  val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  var db: DistributedGraphFacade = _
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))

  var id1: Long = _
  var id2: Long = _
  var id3: Long = _
  var n1: Long = _
  var n2: Long = _
  var n3: Long = _
  var n4: Long = _
  var n5: Long = _
  var m1: Long = _
  var m2: Long = _

  @Before
  def init(): Unit ={
    db = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
//    db.cleanDB()
//
//     id1 = db.addNode(Map("name"->"alex",
//      "storage"->1000000,
//      "salary"->2333.33,
//      "isCoder"->true,
//      "indexes"->Array[Int](1,2,3,4,5),
//      "floats"->Array[Double](11.1, 22.2, 33.3),
//      "bools"->Array[Boolean](true, true, false, false),
//      "jobs"->Array[String]("teacher", "coder", "singer")), "person")
//
//     id2 = db.addNode(Map("name"->"bob",
//      "storage"->1000000,
//      "indexes"->Array[Int](1,2,2,2,2)), "people")
//
//     id3 = db.addNode(Map("name"->"clause",
//      "storage"->66666,
//      "isCoder"->true,
//      "indexes"->Array[Int](1,2,3,4,5)), "person")
//
//    db.addRelation("TMP", id1, id2, Map())
//    db.addRelation("TMP", id2, id3, Map("index"->2))
//
//    n1 = db.addNode(Map("name"->"Oliver Stone", "sex"->"male"), "Person","Director")
//    n2 = db.addNode(Map("name"->"Michael Douglas"), "Person")
//    n3 = db.addNode(Map("name"->"Charlie Sheen"), "Person")
//    n4 = db.addNode(Map("name"->"Martin Sheen"), "Person")
//    n5 = db.addNode(Map("name"->"Rob Reiner"), "Person", "Director")
//    m1 = db.addNode(Map("title"->"Wall Street", "year"->1987), "Movie")
//    m2 = db.addNode(Map("title"->"The American President"), "Movie")
//
//    val directedR1 = db.addRelation("DIRECTED", n1, m1, Map())
//    val directedR2 = db.addRelation("DIRECTED", n5, m2, Map())
//    val actedR1 = db.addRelation("ACTED_IN", n2, m1, Map("role"->"Gordon Gekko"))
//    val actedR2 = db.addRelation("ACTED_IN", n2, m2, Map("role"->"President Andrew Shepherd"))
//    val actedR3 = db.addRelation("ACTED_IN", n3, m1, Map("role"->"Bud Fox"))
//    val actedR4 = db.addRelation("ACTED_IN", n4, m1, Map("role"->"Carl Fox"))
//    val actedR5 = db.addRelation("ACTED_IN", n4, m2, Map("role"->"A.J. MacInerney"))
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

    val res8 = db.cypher("match (n:person)-[r]->(m:people) return r").records().next()("r").asInstanceOf[LynxRelationship]
    Assert.assertEquals(id1, res8.startNodeId.value)
    Assert.assertEquals(id2, res8.endNodeId.value)
    Assert.assertEquals("TMP", res8.relationType.get)

    val res9 = db.cypher("match (n)-[r]->(m) where n.name='Rob Reiner' return r").records().next()("r").asInstanceOf[LynxRelationship]
    Assert.assertEquals(n5, res9.startNodeId.value)
    Assert.assertEquals(m2, res9.endNodeId.value)
    Assert.assertEquals("DIRECTED", res9.relationType.get)

    val res10 = db.cypher("match (n)-[r]->(m) where n.name='XXX' return r").records().toList
    Assert.assertEquals(0, res10.size)
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
