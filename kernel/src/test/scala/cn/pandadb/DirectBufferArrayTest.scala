package cn.pandadb

import cn.pandadb.kernel.impl.{DirectBufferArray}
import cn.pandadb.kernel.store.StoredRelation
import org.junit.{Assert, Test}

class DirectBufferArrayTest {

  @Test
  def putAndGetTest(): Unit ={
    val array = new DirectBufferArray(1024, 8 * 3 + 4)
    val r1 = StoredRelation(1,1,1,1)
    val r2 = StoredRelation(2,1,1,1)
    val r3 = StoredRelation(3,1,1,1)
    val r4 = StoredRelation(4,1,1,1)
    array.put(r1)
    array.put(r2)
    array.put(r3)
    Assert.assertEquals(3, array.size())

    val res = array.get(2L)
    Assert.assertEquals(StoredRelation(2L, 1, 1, 1), res)

    array.put(r4)
    Assert.assertEquals(4, array.size())
  }

  @Test
  def putExpandTest(): Unit ={
    val array = new DirectBufferArray(10, 8 * 3 + 4)
    Assert.assertEquals(1024, array.capacity())

    for (i <- 1 to 58){
      array.put(StoredRelation(i, i, i, i))
    }
    Assert.assertEquals(58, array.size())
    Assert.assertEquals(2048, array.capacity())
  }

  @Test
  def deleteTest(): Unit ={
    val array = new DirectBufferArray(1024, 8 * 3 + 4)
    val r1 = StoredRelation(1,1,1,1)
    val r2 = StoredRelation(2,1,1,1)
    val r3 = StoredRelation(3,1,1,1)
    array.put(r1)
    array.put(r2)
    array.put(r3)
    array.delete(2L)
    array.put(StoredRelation(5,1,1,1))
    Assert.assertEquals(null, array.get(2L))
    Assert.assertEquals(StoredRelation(5,1,1,1), array.get(5L))
  }
}
