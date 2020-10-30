package cn.pandadb

import cn.pandadb.kernel.impl.ByteBufferArray
import org.junit.{Assert, Test}

class ArrayTest {

  @Test
  def putAndGetTest(): Unit ={
    val array = new ByteBufferArray(1024)
    array.put(1L, 1L, 1L, 2L)
    array.put(2L, 2L, 2L, 3L)
    array.put(3L, 3L, 3L, 4L)
    Assert.assertEquals(3, array.size())

    val res = array.get(2L)
    Assert.assertEquals((2L, 2L, 2L, 3L), res._2)

    array.put(4L, 4L, 4L, 5L)
    Assert.assertEquals(4, array.size())
  }

  @Test
  def putExpandTest(): Unit ={
    val array = new ByteBufferArray(10)
    Assert.assertEquals(1024, array.capacity())

    for (i <- 1 to 34){
      array.put(i, i, i, i)
    }
    Assert.assertEquals(34, array.size())
    Assert.assertEquals(2048, array.capacity())
  }

  @Test
  def deleteTest(): Unit ={
    val array = new ByteBufferArray(1024)
    array.put(1L, 1L, 1L, 2L)
    array.put(2L, 2L, 2L, 3L)
    array.put(3L, 3L, 3L, 4L)
    array.put(4L, 4L, 4L, 4L)

    array.delete(2L)
    array.put(5, 5, 5, 5)
    Assert.assertEquals(1, array.get(5)._1)
  }
}
