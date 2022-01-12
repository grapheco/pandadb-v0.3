package cn.pandadb.test.cypher.emb


import org.grapheco.lynx.{ConstrainViolatedException, LynxValue}
import org.junit.function.ThrowingRunnable
import org.junit.{After, Assert, Before, Test}
import java.io.File

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.kernel.udp.{UDPClient, UDPClientManager}

class DeleteNodeTest {
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
    db.cleanDB()

    id1 = db.addNode(Map("name"->"alex",
      "storage"->1000000,
      "salary"->2333.33,
      "isCoder"->true,
      "indexes"->Array[Int](1,2,3,4,5),
      "floats"->Array[Double](11.1, 22.2, 33.3),
      "bools"->Array[Boolean](true, true, false, false),
      "jobs"->Array[String]("teacher", "coder", "singer")), "person")

    id2 = db.addNode(Map("name"->"bob",
      "storage"->1000000,
      "indexes"->Array[Int](1,2,2,2,2)), "people")

    id3 = db.addNode(Map("name"->"clause",
      "storage"->66666,
      "isCoder"->true,
      "indexes"->Array[Int](1,2,3,4,5)), "person")

    db.addRelation("TMP", id1, id2, Map())
    db.addRelation("TMP", id2, id3, Map("index"->2))

    n1 = db.addNode(Map("name"->"Oliver Stone", "sex"->"male"), "Person","Director")
    n2 = db.addNode(Map("name"->"Michael Douglas"), "Person")
    n3 = db.addNode(Map("name"->"Charlie Sheen"), "Person")
    n4 = db.addNode(Map("name"->"Martin Sheen"), "Person")
    n5 = db.addNode(Map("name"->"Rob Reiner"), "Person", "Director")
    m1 = db.addNode(Map("title"->"Wall Street", "year"->1987), "Movie")
    m2 = db.addNode(Map("title"->"The American President"), "Movie")

    val directedR1 = db.addRelation("DIRECTED", n1, m1, Map())
    val directedR2 = db.addRelation("DIRECTED", n5, m2, Map())
    val actedR1 = db.addRelation("ACTED_IN", n2, m1, Map("role"->"Gordon Gekko"))
    val actedR2 = db.addRelation("ACTED_IN", n2, m2, Map("role"->"President Andrew Shepherd"))
    val actedR3 = db.addRelation("ACTED_IN", n3, m1, Map("role"->"Bud Fox"))
    val actedR4 = db.addRelation("ACTED_IN", n4, m1, Map("role"->"Carl Fox"))
    val actedR5 = db.addRelation("ACTED_IN", n4, m2, Map("role"->"A.J. MacInerney"))
  }

  @After
  def close(): Unit ={
    db.close()
  }

  @Test
  def testDeleteNode(): Unit = {

    Assert.assertThrows(classOf[ConstrainViolatedException], new ThrowingRunnable() {
      override def run(): Unit = {
        db.cypher("MATCH (n) Delete n")
      }
    })
  }

  @Test
  def testDeleteDetachNode(): Unit = {
    var res = db.cypher("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(10L, res)
    db.cypher("MATCH (n) Detach Delete n")
    res = db.cypher("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(0L, res)
  }

}
