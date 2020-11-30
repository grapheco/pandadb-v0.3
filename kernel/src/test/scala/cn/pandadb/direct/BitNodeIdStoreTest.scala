package cn.pandadb.direct

import cn.pandadb.kernel.direct.BitNodeIdStore
import org.junit.{Assert, Test}

class BitNodeIdStoreTest {
  @Test
  def test(): Unit = {
    val store = new BitNodeIdStore(8)
    store.setNodeId(1)
    store.setNodeId(64)
    store.setNodeId(65)
    Assert.assertEquals(true, store.exists(1))
    Assert.assertEquals(true, store.exists(64))
    Assert.assertEquals(true, store.exists(65))

    Assert.assertEquals(false, store.exists(32))
    Assert.assertEquals(false, store.exists(33))

    store.reset(64)
    Assert.assertEquals(true, store.exists(1))
    Assert.assertEquals(false, store.exists(64))
    Assert.assertEquals(true, store.exists(65))

    Assert.assertEquals(2, store.directBufferArray.length)

  }

  @Test
  def testBoundary(): Unit = {
    val store = new BitNodeIdStore(1)
    for (i <- 5 to 8) {
      store.setNodeId(i)
    }
    val iter = store.getAllNodeId()

    Assert.assertEquals(4, iter.toStream.length)
    Assert.assertEquals(1, store.directBufferArray.length)

    store.setNodeId(9)
    Assert.assertEquals(2, store.directBufferArray.length)

  }

  @Test
  def iterator(): Unit = {
    val store = new BitNodeIdStore(8)
    store.setNodeId(1)
    store.setNodeId(64)
    store.setNodeId(65)
    val iter = store.getAllNodeId()
    Assert.assertEquals(3, iter.toStream.length)
  }
}
}
