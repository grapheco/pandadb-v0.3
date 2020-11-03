package scala.cn.pandadb

import cn.pandadb.kernel.impl.{BlockManager, DataBlock, Relation}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class BlockDirectBufferArrayDesignTest {
  @Test
  def testSplit(): Unit ={
    val directBuffer = new ArrayBuffer[DataBlock]()
    val manager = new BlockManager(directBuffer, 3)
    manager.put(Relation(1, 1, 1, 1))
    manager.put(Relation(10, 5, 5, 5))
    manager.put(Relation(5, 1, 1, 1))
    directBuffer.foreach(f=>f.dataArray.foreach(println))
    println("======================================")
    manager.put(Relation(2, 1, 1, 1))
    directBuffer.foreach(f=>f.dataArray.foreach(println))
    println("======================================")
  }

  @Test
  def testDelete(): Unit ={
    val directBuffer = new ArrayBuffer[DataBlock]()
    val manager = new BlockManager(directBuffer, 3)
    manager.put(Relation(1, 1, 1, 1))
    manager.put(Relation(10, 5, 5, 5))
    manager.put(Relation(5, 1, 1, 1))
    directBuffer.foreach(f=>f.dataArray.foreach(println))
    println("======================================")
    manager.put(Relation(2, 1, 1, 1))
    directBuffer.foreach(f=>f.dataArray.foreach(println))
    println("======================================")
    manager.delete(2)
    manager.delete(5)
    directBuffer.foreach(f=>f.dataArray.foreach(println))
    println("======================================")
    manager.put(Relation(4, 1, 1, 1))
    manager.put(Relation(6, 1, 1, 1))
    directBuffer.foreach(f=>f.dataArray.foreach(println))
    println("======================================")
  }
}
