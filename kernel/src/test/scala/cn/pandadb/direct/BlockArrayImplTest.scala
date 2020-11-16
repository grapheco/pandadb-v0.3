package scala.cn.pandadb.direct

import cn.pandadb.kernel.direct.{DirectMemoryManager, OutGoingEdgeBlockManager}
import org.junit.{After, Assert, Test}

import scala.util.Random

class BlockArrayTest {

  @Test
  def test1BlockInsertToPre(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(5)

    val blockHead = DirectMemoryManager.getBlock((0, DirectMemoryManager.BLOCK_SIZE))
    val blockNext = DirectMemoryManager.getBlock((0, 0))

    Assert.assertArrayEquals(Array[Long](5,0,0,0,0), blockHead.nodeIdArray)
    Assert.assertArrayEquals(Array[Long](10,20,30,40,50), blockNext.nodeIdArray)
  }
  @Test
  def test1BlockInsertToMiddle(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(25)

    val blockHead = DirectMemoryManager.getBlock((0, DirectMemoryManager.BLOCK_SIZE))
    val blockNext = DirectMemoryManager.getBlock((0, DirectMemoryManager.BLOCK_SIZE * 2))

    Assert.assertArrayEquals(Array[Long](10,20,25,0,0), blockHead.nodeIdArray)
    Assert.assertArrayEquals(Array[Long](30,40,50,0,0), blockNext.nodeIdArray)
  }
  @Test
  def test1BlockInsertToTail(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(60)

    val blockHead = DirectMemoryManager.getBlock((0, 0))
    val blockNext = DirectMemoryManager.getBlock((0, DirectMemoryManager.BLOCK_SIZE))

    Assert.assertArrayEquals(Array[Long](10,20,30,40,50), blockHead.nodeIdArray)
    Assert.assertArrayEquals(Array[Long](60,0,0,0,0), blockNext.nodeIdArray)
  }
  @Test
  def test1BlockDeleteAll(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    for (i <- 1 to 5){
      manager1.delete(i * 10)
    }

    val blockHead = DirectMemoryManager.getBlock((0, 0))
    Assert.assertArrayEquals(Array[Long](0,0,0,0,0), blockHead.nodeIdArray)
  }
  @Test
  def test1BlockDeleteOneData(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.delete(30)

    val blockHead = DirectMemoryManager.getBlock((0, 0))
    Assert.assertArrayEquals(Array[Long](10,20,40,50,0), blockHead.nodeIdArray)
  }

  @Test
  def test3BlocksInsert2Head(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(60)
    manager1.put(70)
    manager1.put(80)
    manager1.put(90)
    manager1.put(100)

    manager1.put(110)
    manager1.put(210)
    manager1.put(310)
    manager1.put(410)
    manager1.put(510)

    manager1.put(25)

    val blockHead = DirectMemoryManager.getBlock((0, DirectMemoryManager.BLOCK_SIZE * 3))
    Assert.assertArrayEquals(Array[Long](10,20,25,0,0), blockHead.nodeIdArray)
    manager1.queryByLink(manager1.getBeginBlockId)
  }
  @Test
  def test3BlocksInsert2Middle(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(60)
    manager1.put(70)
    manager1.put(80)
    manager1.put(90)
    manager1.put(100)

    manager1.put(110)
    manager1.put(210)
    manager1.put(310)
    manager1.put(410)
    manager1.put(510)

    manager1.put(66)

    val blockHead = DirectMemoryManager.getBlock((0, DirectMemoryManager.BLOCK_SIZE * 3))
    Assert.assertArrayEquals(Array[Long](60,66,70,0,0), blockHead.nodeIdArray)
    manager1.queryByLink(manager1.getBeginBlockId)
  }
  @Test
  def test3BlocksInsert2Last(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()
    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(60)
    manager1.put(70)
    manager1.put(80)
    manager1.put(90)
    manager1.put(100)

    manager1.put(110)
    manager1.put(210)
    manager1.put(310)
    manager1.put(410)
    manager1.put(510)

    manager1.put(444)

    val blockHead = DirectMemoryManager.getBlock((0, DirectMemoryManager.BLOCK_SIZE * 4))
    Assert.assertArrayEquals(Array[Long](410, 444,510,0,0), blockHead.nodeIdArray)
    manager1.queryByLink(manager1.getBeginBlockId)
  }

  @Test
  def test2InsertSplitBlock(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()
    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(60)
    manager1.put(70)
    manager1.put(80)
    manager1.put(90)
    manager1.put(100)

    manager1.put(110)
    manager1.put(210)
    manager1.put(310)
    manager1.put(410)
    manager1.put(510)

    manager1.put(444)
    manager1.put(445)
    //    val blockHead = DirectMemoryManager.getBlock((0, DirectMemoryManager.BLOCK_SIZE * 4))
    //    Assert.assertArrayEquals(Array[Long](410, 444,445,510,0), blockHead.nodeIdArray)
    manager1.queryByLink(manager1.getBeginBlockId)
  }

  @Test
  def testRandomData(): Unit ={
    var dataSet = Set[Int]()
    for (i <- 1 to 1000){
      dataSet += Random.nextInt(10000)
    }
    val manager = new OutGoingEdgeBlockManager()
    dataSet.foreach(f=>manager.put(f))
    manager.queryByLink(manager.getBeginBlockId)
  }

  @After
  def clear(): Unit ={
    Thread.sleep(100)
    DirectMemoryManager.directBufferPageArray.foreach(f=>f.release())
    DirectMemoryManager.directBufferPageArray.clear()
    DirectMemoryManager.pageId = -1
    DirectMemoryManager.currentPageBlockId = 0
  }
}
