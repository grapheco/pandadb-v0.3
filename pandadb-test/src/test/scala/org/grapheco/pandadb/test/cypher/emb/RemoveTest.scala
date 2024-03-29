package org.grapheco.pandadb.test.cypher.emb

import java.io.File

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
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
    db = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
    db.cleanDB()
    val n1 = db.addNode(Map("name"->"Oliver Stone", "sex"->"male", "value1"->1, "value2"->true), "Person","Director")
    val m1 = db.addNode(Map("title"->"Wall Street", "year"->1987), "Movie")
    val directedR1 = db.addRelation("DIRECTED", n1, m1, Map("value1"->1, "value2"->2, "value3"->3))
  }

  @Test
  def testRemoveNodeProperty(): Unit ={
    val res = db.cypher("match (n) where n.name='Oliver Stone' remove n.value1 return n").records().next()("n").asInstanceOf[PandaNode].props
    Assert.assertEquals(Seq("Oliver Stone", "male", true), res.values.map(v => v.value).toSeq)
  }

  @Test
  def testRemoveNodeLabel(): Unit ={
    val res = db.cypher("match (n) where n.name='Oliver Stone' remove n:Director return n").records().next()("n").asInstanceOf[PandaNode].labels.map(_.value)
    Assert.assertEquals(Seq("Person"), res)
  }

  @Test
  def testRemoveRelationshipProperty(): Unit ={
    val res = db.cypher("match (n)-[r]->(m) remove r.value2, r.value3 return r").records().next()("r").asInstanceOf[PandaRelationship].props
    Assert.assertEquals(Seq(1), res.values.map(v => v.value).toSeq)
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
