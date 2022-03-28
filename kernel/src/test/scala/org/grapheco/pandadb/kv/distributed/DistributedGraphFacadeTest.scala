package org.grapheco.pandadb.kv.distributed

import java.nio.ByteBuffer

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.store.StoredNodeWithProperty
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.grapheco.lynx.{LynxInteger, LynxString}
import org.junit.{After, Assert, Before, Test}
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 18:06
 */
class DistributedGraphFacadeTest {

  var api: DistributedGraphFacade = _

  var tikv: RawKVClient = _

  val kvHosts = "10.0.82.144:2379,10.0.82.145:2379,10.0.82.146:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))
  @Before
  def init(): Unit = {
    val conf = TiConfiguration.createRawDefault(kvHosts)
    val session = TiSession.create(conf)
    tikv = session.createRawClient()
//    cleanDB()

    api = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
//    addData()
  }

  def addData(): Unit = {
    api.addNode(Map("name" -> "a1", "age" -> 11), "person", "worker")
    api.addNode(Map("name" -> "a2", "age" -> 12, "country" -> "China"), "person", "human")
    api.addNode(Map("name" -> "a3", "age" -> 13), "person", "CNIC")
    api.addNode(Map("name" -> "a4", "age" -> 12), "person", "worker")
    api.addNode(Map("name" -> "a5", "age" -> 15), "person", "man")

    api.addRelation("friend1", 1, 2, Map.empty)
    api.addRelation("friend2", 2, 3, Map.empty)
    api.addRelation("friend3", 4, 5, Map.empty)
    api.addRelation("friend4", 4, 2, Map("Year" -> 2020))
  }

  @Test
  def cypherTest(): Unit ={
    api.cypher(
      """
        |MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject) RETURN t.scientific_name as scientific_name, b.bioproject_id as bioproject_id, b.title as title, b.cen as cen SKIP 0 LIMIT 10
        |""".stripMargin).show()
  }

  def cleanDB(): Unit ={
    val left = ByteString.copyFrom(ByteBuffer.wrap(Array((0).toByte)))
    val right = ByteString.copyFrom(ByteBuffer.wrap(Array((-1).toByte)))
    tikv.deleteRange(left, right)
  }

  @Test
  def all(): Unit = {
    val nodes = api.scanAllNodes()
    val rels = api.scanAllRelations()
    Assert.assertEquals(5, nodes.size)
    Assert.assertEquals(4, rels.size)
  }

  @Test
  def getNodesByLabel(): Unit ={
    val iter1 = api.getNodesByLabel("person", false)
    val iter2 = api.getNodesByLabel("worker", false)

    Assert.assertEquals(5, iter1.size)
    Assert.assertEquals(2, iter2.size)

  }

  @Test
  def cypher(): Unit = {
    val iter = api.cypher("match (n:person) return n").records()
    Assert.assertEquals(5, iter.size)
  }

  @Test
  def deleteNode(): Unit = {
    api.deleteNode(2)
    Assert.assertEquals(None, api.getNodeById(2))
  }

  @Test
  def nodeAddLabel(): Unit = {
    api.nodeAddLabel(1, "test")
    Assert.assertEquals(Seq("person", "worker", "test"), api.getNodeById(1).get.labels.map(_.value))
  }

  @Test
  def nodeRemoveLabel(): Unit = {
    api.nodeAddLabel(1, "test")
    Assert.assertEquals(Seq("person", "worker", "test"), api.getNodeById(1).get.labels.map(_.value))

    api.nodeRemoveLabel(1, "test")
    Assert.assertEquals(Seq("person", "worker"), api.getNodeById(1).get.labels.map(_.value))
  }

  @Test
  def nodeSetProperty(): Unit = {
    api.nodeSetProperty(1, "TestKey", "testValue")
    Assert.assertEquals(Seq(("name", LynxString("a1")), ("age", LynxInteger(11)), ("TestKey", LynxString("testValue"))),
      api.getNodeById(1).get.props.map{ case (key, value) => (key.value, value)}.toSeq)
  }

  @Test
  def nodeRemoveProperty(): Unit = {
    api.nodeRemoveProperty(1, "TestKey")
    Assert.assertEquals(Seq(("name", LynxString("a1")), ("age", LynxInteger(11))),
      api.getNodeById(1).get.props.map{ case (key, value) => (key.value, value)}.toSeq)
  }

  @Test
  def relationSetProperty(): Unit = {
    api.relationSetProperty(1, "Color", "blue")
    Assert.assertEquals(Map("Color" -> LynxString("blue")), api.getRelationById(1).get.props.map{ case (key, value) => (key.value, value)})
  }

  @Test
  def relationRemoveProperty(): Unit = {
    api.relationRemoveProperty(1, "Color")
    Assert.assertEquals(Map.empty, api.getRelationById(1).get.props.map{ case (key, value) => (key.value, value)})
  }

  @After
  def close(): Unit = {
    api.close()
  }
}
