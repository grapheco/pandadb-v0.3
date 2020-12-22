//package cn.pandadb.util.serializer
//
//import cn.pandadb.kernel.kv.node.NodeValue
//import cn.pandadb.kernel.util.Profiler.timing
//import cn.pandadb.kernel.util.serializer.NodeSerializer
//import org.junit.{Assert, Test}
//
///**
// * @Author: Airzihao
// * @Description:
// * @Date: Created at 16:31 2020/12/17
// * @Modified By:
// */
//class NodeSerializerTest {
//  val nodeValue = new NodeValue(123456, Array(1), Map(1->1, 2->"dasd", 3->true))
//  val nodeValueSerializer = NodeSerializer
//
//  @Test
//  def serializePERF(): Unit = {
//    println("serialize")
//    timing(for (i<-1 to 10000000) nodeValueSerializer.serialize(nodeValue))
//  }
//  @Test
//  def deSerializePERF(): Unit = {
//    println("deserialize")
//    val bytesArr = nodeValueSerializer.serialize(nodeValue)
//    timing(for (i<-1 to 10000000) nodeValueSerializer.deserializeNodeValue(bytesArr))
//  }
//
//  @Test
//  def correntTest(): Unit = {
//    val valueBytes = nodeValueSerializer.serialize(nodeValue)
//    val keyBytes = nodeValueSerializer.serialize(nodeValue.id)
//    val node = nodeValueSerializer.deserializeNodeValue(valueBytes)
//    val id = nodeValueSerializer.deserializeNodeKey(keyBytes)
//    Assert.assertEquals(nodeValue.id, node.id)
//    Assert.assertArrayEquals(nodeValue.labelIds, node.labelIds)
//    Assert.assertEquals(nodeValue.properties, node.properties)
//    Assert.assertEquals(nodeValue.id, id)
//  }
//}
