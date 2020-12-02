package cn.pandadb.kv
import cn.pandadb.kernel.kv.KeyHandler
import cn.pandadb.kernel.kv.KeyHandler.KeyType
import org.junit.{After, Assert, Before, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:59 2020/11/28
 * @Modified By:
 */
class KeyHandlerTest {
  val id1: Long = 12345678
  val id2: Long = 87654321
  val category: Long = 88888888
  val labelId: Int = 6666

//  val inEdgeKey: Array[Byte] = KeyHandler.inEdgeKeyToBytes(id1, id2, labelId, category)
//  val inEdgeKey2: Array[Byte] = KeyHandler.inEdgeKeyToBytes(id1, id2, labelId, category)
//  val outEdgeKey: Array[Byte] = KeyHandler.outEdgeKeyToBytes(id1, id2, labelId, category)

//  @Test
//  def testTwinEdge(): Unit ={
//    Assert.assertArrayEquals(inEdgeKey, KeyHandler.twinEdgeKey(outEdgeKey))
//    Assert.assertArrayEquals(outEdgeKey, KeyHandler.twinEdgeKey(inEdgeKey))
//  }
//
//  @Test
//  def testBytesToEdge: Unit = {
//    val inEdgeTuple = (KeyType.InEdge.id.toByte, id1, labelId, category, id2)
//    val outEdgeTuple = (KeyType.OutEdge.id.toByte, id2, labelId, category, id1)
//    Assert.assertEquals(inEdgeTuple, KeyHandler.bytesToEdge(inEdgeKey))
//    Assert.assertEquals(outEdgeTuple, KeyHandler.bytesToEdge(outEdgeKey))
//  }
}
