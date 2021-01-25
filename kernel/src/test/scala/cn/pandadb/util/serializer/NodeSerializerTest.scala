package cn.pandadb.util.serializer

import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.Profiler.timing
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 16:31 2020/12/17
 * @Modified By:
 */
class NodeSerializerTest {
  val nodeValue = new StoredNodeWithProperty(123456, Array(1), Map(1->1, 2->"dasd", 3->true))
  val nodeValueSerializer = NodeSerializer

  @Test
  def serializePERF(): Unit = {
    println("serialize")
    timing(for (i<-1 to 10000000) nodeValueSerializer.serialize(nodeValue))
  }
  @Test
  def deSerializePERF(): Unit = {
    println("deserialize")
    val bytesArr = nodeValueSerializer.serialize(nodeValue)
    timing(for (i<-1 to 10000000) nodeValueSerializer.deserializeNodeValue(bytesArr))
  }

  @Test
  def correntTest(): Unit = {
    val valueBytes = nodeValueSerializer.serialize(nodeValue)
    val keyBytes = nodeValueSerializer.serialize(nodeValue.id)
    val node = nodeValueSerializer.deserializeNodeValue(valueBytes)
    val id = nodeValueSerializer.deserializeNodeKey(keyBytes)
    Assert.assertEquals(nodeValue.id, node.id)
    Assert.assertArrayEquals(nodeValue.labelIds, node.labelIds)
    Assert.assertEquals(nodeValue.properties, node.properties)
    Assert.assertEquals(nodeValue.id, id)
  }

  @Test
  def longStrTest(): Unit = {
    val shortStr = "abcdefghijklmnopqrstuvwxyz"
    val longStr = "abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz"
    val shortNode = new StoredNodeWithProperty(123456, Array(1), Map(1 -> shortStr))
    val longNode = new StoredNodeWithProperty(123456, Array(1), Map(1 -> longStr))
    val shortBytes = nodeValueSerializer.serialize(shortNode)
    val longBytes = nodeValueSerializer.serialize(longNode)
    val node1 = nodeValueSerializer.deserializeNodeValue(shortBytes)
    val node2 = nodeValueSerializer.deserializeNodeValue(longBytes)
    Assert.assertEquals(longStr, node2.properties(1))
  }
}
