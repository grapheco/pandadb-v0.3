package scala.cn.pandadb.direct

import scala.collection.mutable
import cn.pandadb.kernel.direct.{FromNodeBlockManager, GlobalManager}
import org.junit.{Assert, Before, Test}

import scala.util.Random


class BlockDirectBufferArrayDesignTest {

  var globalManager: GlobalManager = null

  @Before
  def init(): Unit ={
    globalManager = new GlobalManager()
  }

  @Test
  def fromNodeOnlyHaveOneEndNodesBlock(): Unit ={
    val manager = new FromNodeBlockManager(globalManager)
    manager.put(10)
    manager.put(20)
    manager.put(30)
    Assert.assertEquals(1, globalManager.directBuffer.size)

    // left
    manager.put(5)
    Assert.assertEquals(1, manager.getBeginBlockIndex())
    Assert.assertEquals(2, globalManager.directBuffer.size)
    manager.delete(5)
    Assert.assertEquals(1, globalManager.deleteLog(0))


    //middle
    manager.put(16)
    Assert.assertEquals(2, globalManager.directBuffer.size)
    manager.delete(16)
    manager.put(4)

    // right
    manager.put(40)
    manager.queryByLink()
  }
  @Test
  def randomTest(): Unit ={
    val manager = new FromNodeBlockManager(globalManager)
    val dataSet = mutable.Set[Int]()
    for (i <- 1 to 1000){
      dataSet += Random.nextInt(1000000)
    }
    dataSet.foreach(f => manager.put(f))

    manager.queryByLink()
  }
}
