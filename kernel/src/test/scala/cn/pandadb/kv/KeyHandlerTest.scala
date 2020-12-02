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
  @Test
  def testNodeKey(): Unit = {
    val nodeId: Long = 123
    val bytes = KeyHandler.nodeKeyToBytes(nodeId)
    Assert.assertEquals(9, bytes.length)
    Assert.assertEquals(nodeId, KeyHandler.parseNodeKeyFromBytes(bytes))
  }

  @Test
  def testNodeLabelIndexKey(): Unit = {
    val label: Int = 888
    val nodeId: Long = 456
    val bytes = KeyHandler.nodeLabelIndexKeyToBytes(label, nodeId)
    Assert.assertEquals(13, bytes.length)
    Assert.assertEquals(label, KeyHandler.parseNodeLabelIndexKeyFromBytes(bytes)._1)
    Assert.assertEquals(nodeId, KeyHandler.parseNodeLabelIndexKeyFromBytes(bytes)._2)
  }

}
