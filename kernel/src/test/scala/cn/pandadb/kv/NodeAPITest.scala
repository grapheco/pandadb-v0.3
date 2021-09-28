package cn.pandadb.kv

import java.io.File
import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, StoredNodeWithProperty}
import org.apache.commons.io.IOUtils
import org.grapheco.lynx.cypherplus.Blob
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

import java.net.URL

/**
 * @ClassName NodeAPITest
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/24
 * @Version 0.1
 */
@Test
class NodeAPITest {

  var nodeAPI: NodeStoreSPI = _
  val path = "testdata/rocksdb"
  val node1 = new StoredNodeWithProperty(1,Array(1),Map(0->"bob", 1->22, 2-> 66.7, 3->true))
  val node2 = new StoredNodeWithProperty(2,Array(2),Map(0->"tom", 1->24, 2-> 6.7, 3->false))
  val node3 = new StoredNodeWithProperty(3,Array(1),Map(0->"jack", 1->23, 2-> 66.000007, 3->true))
  val node4 = new StoredNodeWithProperty(4,Array(2),Map(0->"jerry", 1->2, 2-> 6.7, 3->false))
  val node5 = new StoredNodeWithProperty(5,Array(1,2),Map(0->"pig", 1->2, 2-> 0.7, 3->true))
  val url = "https://www.baidu.com/img/flexible/logo/pc/result.png"
  val surl = new URL(url)
  val node6 = new StoredNodeWithProperty(id = 6,Array(10),Map(0->"baidu", 2->Blob.fromHttpURL(url)))

  @Before
  def init(): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      dir.delete()
    }
    nodeAPI = new NodeStoreAPI(path)
    nodeAPI.addNode(node1)
    nodeAPI.addNode(node2)
    nodeAPI.addNode(node3)
    nodeAPI.addNode(node4)
    nodeAPI.addNode(node5)
    nodeAPI.addNode(node6)
  }

  @After
  def end(): Unit = {
    nodeAPI.close()
  }

  @Test
  def getTest(): Unit ={
    // get props and labels
    Assert.assertEquals(66.7, nodeAPI.getNodeById(1).get.properties(2))
    Assert.assertEquals("tom", nodeAPI.getNodeById(2).get.properties(0))
    Assert.assertEquals(23, nodeAPI.getNodeById(3).get.properties(1))
    Assert.assertEquals(false, nodeAPI.getNodeById(4).get.properties(3))
    Assert.assertArrayEquals(Array(1,2), nodeAPI.getNodeById(5).get.labelIds)
    Assert.assertArrayEquals(IOUtils.toByteArray(surl), nodeAPI.getNodeById(6).get.properties(2).asInstanceOf[Blob].toBytes())
    Assert.assertEquals(None, nodeAPI.getNodeById(7))

    // get nodes by labels
    Assert.assertArrayEquals(Array[Long](1,3,5), nodeAPI.getNodeIdsByLabel(1).toArray)
    Assert.assertArrayEquals(Array[Long](1,2,3,4,5,6), nodeAPI.allNodes().toArray.map(_.id).sorted)
  }

  @Test
  def updateTest(): Unit ={
    // label
    nodeAPI.nodeAddLabel(1, 3)
    Assert.assertArrayEquals(Array(1,3), nodeAPI.getNodeById(1).get.labelIds)
    nodeAPI.nodeRemoveLabel(5, 1)
    Assert.assertArrayEquals(Array(2), nodeAPI.getNodeById(5).get.labelIds)
    Assert.assertArrayEquals(Array[Long](1,3), nodeAPI.getNodeIdsByLabel(1).toArray)
    // props
    nodeAPI.nodeSetProperty(1, 0, "BBB")
    nodeAPI.nodeSetProperty(1, 8, "new Prop")
    Assert.assertEquals("BBB", nodeAPI.getNodeById(1).get.properties(0))
    Assert.assertEquals("new Prop", nodeAPI.getNodeById(1).get.properties(8))
    nodeAPI.nodeRemoveProperty(1, 2)
    Assert.assertEquals(None, nodeAPI.getNodeById(1).get.properties.get(2))
  }

  @Test
  def deleteTest(): Unit ={
    nodeAPI.deleteNode(1)
    Assert.assertEquals(None, nodeAPI.getNodeById(1))
    Assert.assertArrayEquals(Array[Long](3,5), nodeAPI.getNodeIdsByLabel(1).toArray)
    nodeAPI.deleteNodesByLabel(2)
    Assert.assertEquals(None, nodeAPI.getNodeById(2))
    Assert.assertEquals(None, nodeAPI.getNodeById(4))
    Assert.assertEquals(None, nodeAPI.getNodeById(5))
    Assert.assertArrayEquals(Array[Long](3), nodeAPI.getNodeIdsByLabel(1).toArray)
  }
}
