package cn.pandadb.kv.distributed.store

import java.nio.ByteBuffer

import cn.pandadb.kernel.distribute.{DistributedKeyConverter, PandaDistributeKVAPI}
import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import cn.pandadb.kernel.distribute.meta.{NameMapping, PropertyNameStore}
import cn.pandadb.kernel.distribute.node.{DistributedNodeStoreSPI, NodeStoreAPI}
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.udp.UDPClient
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.junit.{Assert, Before, Test}
import org.tikv.common.types.Charset
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 09:38
 */
class NodeStoreAPITest {
  var api: DistributedNodeStoreSPI = _
  var tikv: RawKVClient = _

  val n1 = new StoredNodeWithProperty(1, Array(1), Map(1 -> "a1", 2 -> 181))
  val n2 = new StoredNodeWithProperty(2, Array(1, 2), Map(1 -> "a2", 2 -> 182))
  val n3 = new StoredNodeWithProperty(3, Array(1, 2, 3), Map(1 -> "a3", 2 -> 183))
  val n4 = new StoredNodeWithProperty(4, Array(1, 3), Map(1 -> "a4", 2 -> 184))
  val n5 = new StoredNodeWithProperty(5, Array(2, 3), Map(1 -> "a5", 2 -> 185))
  val n6 = new StoredNodeWithProperty(6, Array(3), Map(1 -> "a6", 2 -> 186))
  val n7 = new StoredNodeWithProperty(7, Array(2), Map(1 -> "a7", 2 -> 187))

  val nodeArray = Array(n1, n2, n3, n4, n5, n6, n7)

  val udpClient = Array(new UDPClient("127.0.0.1", 6000))

  @Before
  def init(): Unit = {
    val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
    val session = TiSession.create(conf)
    tikv = session.createRawClient()

    cleanDB()

    val db = new PandaDistributeKVAPI(tikv)
    api = new NodeStoreAPI(db, new PropertyNameStore(db, udpClient))

    api.addNode(n1)
    api.addNode(n2)
    api.addNode(n3)
    api.addNode(n4)
    api.addNode(n5)
    api.addNode(n6)
    api.addNode(n7)
  }

  def cleanDB(): Unit ={
    val left = ByteString.copyFrom(ByteBuffer.wrap(Array((0).toByte)))
    val right = ByteString.copyFrom(ByteBuffer.wrap(Array((-1).toByte)))
    tikv.deleteRange(left, right)
  }

  @Test
  def allNodes(): Unit = {
    api.allNodes().toArray.sortBy(n => n.id).zipWithIndex.foreach(nodeAndIndex => {
      Assert.assertEquals(nodeArray(nodeAndIndex._2), nodeAndIndex._1)
    })
  }

  @Test
  def deleteNode(): Unit = {
    val array = nodeArray.tail
    api.deleteNode(n1.id)
    api.allNodes().toArray.sortBy(n => n.id).zipWithIndex.foreach(nodeAndIndex => {
      Assert.assertEquals(array(nodeAndIndex._2), nodeAndIndex._1)
    })

    Assert.assertEquals(0, api.getNodeLabelsById(n1.id).length)
  }

  @Test
  def deleteNodes(): Unit = {
    val array = Array(n2, n4, n7)
    val deleteArray = Array(n1, n3, n5, n6)

    api.deleteNodes(deleteArray.map(n => n.id).iterator)
    api.allNodes().toArray.sortBy(n => n.id).zipWithIndex.foreach(nodeAndIndex => {
      Assert.assertEquals(array(nodeAndIndex._2), nodeAndIndex._1)
    })

    deleteArray.foreach(nid => Assert.assertEquals(0, api.getNodeLabelsById(nid.id).length))
  }

  @Test
  def deleteNodesByLabel(): Unit = {
    api.deleteNodesByLabel(3)

    val array = Array(n1, n2, n7)
    api.allNodes().toArray.sortBy(n => n.id).zipWithIndex.foreach(nodeAndIndex => {
      Assert.assertEquals(array(nodeAndIndex._2), nodeAndIndex._1)
    })
  }

  @Test
  def nodeAddLabel(): Unit ={
    api.nodeAddLabel(n1.id, 233)
    Assert.assertArrayEquals(n1.labelIds ++ Array(233), api.getNodeById(n1.id).get.labelIds)
  }
  @Test
  def nodeRemoveLabel(): Unit ={
    api.nodeRemoveLabel(n3.id, 3)
    Assert.assertArrayEquals(Array(1,2), api.getNodeById(n3.id).get.labelIds)
  }

  @Test
  def nodeSetProperty(): Unit ={
    api.nodeSetProperty(n1.id, 233, "China")
    Assert.assertEquals((n1.properties ++ Map(233 -> "China")), api.getNodeById(n1.id).get.properties)
  }
  @Test
  def nodeRemoveProperty(): Unit ={
    api.nodeRemoveProperty(n1.id, 1)
    Assert.assertEquals(n1.properties -- Array(1), api.getNodeById(n1.id).get.properties)
  }

  @Test
  def deleteAll(): Unit = {
    val left = ByteString.copyFrom(ByteBuffer.wrap(Array((0).toByte)))
    val right = ByteString.copyFrom(ByteBuffer.wrap(Array((-1).toByte)))
    tikv.deleteRange(left, right)
  }

  @Test
  def getNodeCount(): Unit = {
    Assert.assertEquals(7, api.nodesCount)
  }
}
