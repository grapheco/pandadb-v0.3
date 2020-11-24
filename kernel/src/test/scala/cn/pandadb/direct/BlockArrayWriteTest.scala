package cn.pandadb.direct

import cn.pandadb.kernel.direct.{DirectMemoryManager, OutGoingEdgeBlockManager}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class BlockArrayWriteTest {
  val managers: ArrayBuffer[OutGoingEdgeBlockManager] = new ArrayBuffer[OutGoingEdgeBlockManager]()

  // data length = 1000 : 6.5s
  DirectMemoryManager.DATA_LENGTH = 1000
  @Test
  def test1yiData(): Unit ={
    val startTime = System.currentTimeMillis()
    for (i <- 1 to 100000){
      val manager = new OutGoingEdgeBlockManager()
      for (j <- 1 to 1000){
        val nodeId = Random.nextLong()
        manager.put(nodeId)
      }
      managers += manager
      if (i % 1000 == 0) println(s"write ${i/1000}/100...")
    }
    println(s"write 1yi cost: ${System.currentTimeMillis() - startTime}")
  }

}
