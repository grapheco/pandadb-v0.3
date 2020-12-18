package cn.pandadb.util

import cn.pandadb.kernel.kv.NodeValue
import cn.pandadb.kernel.util.NodeValueSerializer
import cn.pandadb.kernel.util.Profiler.timing
import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 16:31 2020/12/17
 * @Modified By:
 */
class NodeValueSerializerTest {
  val nodeValue = new NodeValue(123456, Array(1), Map(1->1, 2->"dasd", 3->true))
  val nodeValueSerializer = new NodeValueSerializer

  @Test
  def serializePERF(): Unit = {
    println("serialize")
    timing(for (i<-1 to 10000000) nodeValueSerializer.serialize(nodeValue))
  }
  @Test
  def deSerializePERF(): Unit = {
    println("deserialize")
    val bytesArr = nodeValueSerializer.serialize(nodeValue)
    timing(for (i<-1 to 10000000) nodeValueSerializer.deserialize(bytesArr))

  }

  @Test
  def correntTest(): Unit = {
    val bytesArr = nodeValueSerializer.serialize(nodeValue)
    val node = nodeValueSerializer.deserialize(bytesArr)
    Assert.assertEquals(nodeValue.id, node.id)
    Assert.assertArrayEquals(nodeValue.labelIds, node.labelIds)
    Assert.assertEquals(nodeValue.properties, node.properties)
  }
}
