//package cn.pandadb.util.serializer
//
//import cn.pandadb.kernel.kv.node.NodeValue
//import cn.pandadb.kernel.util.serializer.ChillSerializer
//import org.junit.{Assert, Test}
//
///**
// * @Author: Airzihao
// * @Description:
// * @Date: Created at 10:56 2020/12/17
// * @Modified By:
// */
//class ChillSerializerTest {
//  val chillSerializer = ChillSerializer
//
//  @Test
//  def test() = {
//    val nodeValue = new NodeValue(1, Array(1), Map(1->123, 2->"qwe"))
//    val bytes = chillSerializer.serialize(nodeValue)
//    val dNodeValue = chillSerializer.deserialize(bytes, classOf[NodeValue])
//    Assert.assertEquals(nodeValue.id, dNodeValue.id)
//    Assert.assertArrayEquals(nodeValue.labelIds, dNodeValue.labelIds)
//    Assert.assertEquals(nodeValue.properties, dNodeValue.properties)
//  }
//
//}
