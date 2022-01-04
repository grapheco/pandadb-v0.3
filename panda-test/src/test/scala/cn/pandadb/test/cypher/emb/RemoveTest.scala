package cn.pandadb.test.cypher.emb

import java.io.File

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.kernel.store.{PandaNode, PandaRelationship}
import cn.pandadb.net.udp.UDPClient
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-05-20 10:52
 */
class RemoveTest {
  val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  var db: DistributedGraphFacade = _
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))

  @Before
  def init(): Unit ={
    db = new DistributedGraphFacade(kvHosts, indexHosts, udpClient)
    db.cleanDB()
    val n1 = db.addNode(Map("name"->"Oliver Stone", "sex"->"male", "value1"->1, "value2"->true), "Person","Director")
    val m1 = db.addNode(Map("title"->"Wall Street", "year"->1987), "Movie")
    val directedR1 = db.addRelation("DIRECTED", n1, m1, Map("value1"->1, "value2"->2, "value3"->3))
  }

  @Test
  def testRemoveNodeProperty(): Unit ={
    val res = db.cypher("match (n) where n.name='Oliver Stone' remove n.value1 return n").records().next()("n").asInstanceOf[PandaNode].properties
    Assert.assertEquals(Seq("Oliver Stone", "male", true), res.values.toSeq.map(v => v.value))
  }

  @Test
  def testRemoveNodeLabel(): Unit ={
    val res = db.cypher("match (n) where n.name='Oliver Stone' remove n:Director return n").records().next()("n").asInstanceOf[PandaNode].labels
    Assert.assertEquals(Seq("Person"), res)
  }

  @Test
  def testRemoveRelationshipProperty(): Unit ={
    val res = db.cypher("match (n)-[r]->(m) remove r.value2, r.value3 return r").records().next()("r").asInstanceOf[PandaRelationship].properties
    Assert.assertEquals(Seq(1), res.values.toSeq.map(v => v.value))
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
