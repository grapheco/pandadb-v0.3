package scala.cn.pandadb.direct

import scala.collection.mutable
import cn.pandadb.kernel.direct.{BlockManager, DataBlock, Relation}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class BlockDirectBufferArrayDesignTest {
  @Test
  def watchData(): Unit ={
    val directBuffer = new ArrayBuffer[DataBlock]()
    val manager = new BlockManager(directBuffer, 5)
    val dataSet = mutable.Set[Int]()
    for (i <- 1 to 1000){
      dataSet += Random.nextInt(10000)
    }
    dataSet.foreach(f => manager.put(Relation(f,1,1,1)))
    directBuffer.foreach(f=>f.dataArray.foreach(println))
  }
}
