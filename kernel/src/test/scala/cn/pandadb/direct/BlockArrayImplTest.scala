package scala.cn.pandadb.direct

import cn.pandadb.kernel.direct.{BlockId, DirectMemoryManager, OutGoingEdgeBlockManager}
import org.junit.{Assert, Test}

// data length = 5
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

    val blockHead = DirectMemoryManager.getBlockInfo(manager1.getBeginBlockId)
    val blockNextId = blockHead.nextBlockId
    val headArray = DirectMemoryManager.getBlockDataArray(blockHead.blockId)
    val nextArray = DirectMemoryManager.getBlockDataArray(blockNextId)

    Assert.assertEquals(Set(5), headArray.toSet)
    Assert.assertEquals(Set(10,20,30,40,50), nextArray.toSet)
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

    val blockHead = DirectMemoryManager.getBlockInfo(manager1.getBeginBlockId)
    val blockNextId = blockHead.nextBlockId
    val headArray = DirectMemoryManager.getBlockDataArray(blockHead.blockId)
    val nextArray = DirectMemoryManager.getBlockDataArray(blockNextId)

    Assert.assertEquals(Set(10,20,25), headArray.toSet)
    Assert.assertEquals(Set(30,40,50), nextArray.toSet)
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

    val blockHead = DirectMemoryManager.getBlockInfo(manager1.getBeginBlockId)
    val blockNextId = blockHead.nextBlockId
    val headArray = DirectMemoryManager.getBlockDataArray(blockHead.blockId)
    val nextArray = DirectMemoryManager.getBlockDataArray(blockNextId)

    Assert.assertEquals(Set(10,20,30,40,50),headArray.toSet)
    Assert.assertEquals(Set(60), nextArray.toSet)
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

    Assert.assertEquals(BlockId(), manager1.getBeginBlockId)
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

    val blockArray = DirectMemoryManager.getBlockDataArray(manager1.getBeginBlockId)
    Assert.assertEquals(Set(10,20,40,50), blockArray.toSet)
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

    val blockHead = DirectMemoryManager.getBlock(manager1.getBeginBlockId)
    val blockNext1 = DirectMemoryManager.getBlock(blockHead.thisBlockNextBlockId)
    val blockNext2 = DirectMemoryManager.getBlock(blockNext1.thisBlockNextBlockId)
    val blockNext3 = DirectMemoryManager.getBlock(blockNext2.thisBlockNextBlockId)

    val a1 = DirectMemoryManager.getBlockDataArray(blockHead.thisBlockId)
    val a2 = DirectMemoryManager.getBlockDataArray(blockNext1.thisBlockId)
    val a3 = DirectMemoryManager.getBlockDataArray(blockNext2.thisBlockId)
    val a4 = DirectMemoryManager.getBlockDataArray(blockNext3.thisBlockId)

    Assert.assertEquals(Set(10,20,25), a1.toSet)
    Assert.assertEquals(Set(30,40,50), a2.toSet)
    Assert.assertEquals(Set(60,70,80,90,100), a3.toSet)
    Assert.assertEquals(Set(110,210,310,410,510), a4.toSet)
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

    val blockHead = DirectMemoryManager.getBlock(manager1.getBeginBlockId)
    val blockNext1 = DirectMemoryManager.getBlock(blockHead.thisBlockNextBlockId)
    val blockNext2 = DirectMemoryManager.getBlock(blockNext1.thisBlockNextBlockId)
    val blockNext3 = DirectMemoryManager.getBlock(blockNext2.thisBlockNextBlockId)

    val a1 = DirectMemoryManager.getBlockDataArray(blockHead.thisBlockId)
    val a2 = DirectMemoryManager.getBlockDataArray(blockNext1.thisBlockId)
    val a3 = DirectMemoryManager.getBlockDataArray(blockNext2.thisBlockId)
    val a4 = DirectMemoryManager.getBlockDataArray(blockNext3.thisBlockId)

    Assert.assertEquals(Set(10,20,30,40,50), a1.toSet)
    Assert.assertEquals(Set(60,66,70), a2.toSet)
    Assert.assertEquals(Set(80,90,100), a3.toSet)
    Assert.assertEquals(Set(110,210,310,410,510), a4.toSet)
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

    val blockHead = DirectMemoryManager.getBlock(manager1.getBeginBlockId)
    val blockNext1 = DirectMemoryManager.getBlock(blockHead.thisBlockNextBlockId)
    val blockNext2 = DirectMemoryManager.getBlock(blockNext1.thisBlockNextBlockId)
    val blockNext3 = DirectMemoryManager.getBlock(blockNext2.thisBlockNextBlockId)

    val a1 = DirectMemoryManager.getBlockDataArray(blockHead.thisBlockId)
    val a2 = DirectMemoryManager.getBlockDataArray(blockNext1.thisBlockId)
    val a3 = DirectMemoryManager.getBlockDataArray(blockNext2.thisBlockId)
    val a4 = DirectMemoryManager.getBlockDataArray(blockNext3.thisBlockId)


    Assert.assertEquals(Set(10,20,30,40,50), a1.toSet)
    Assert.assertEquals(Set(60,70,80,90,100), a2.toSet)
    Assert.assertEquals(Set(110,210,310), a3.toSet)
    Assert.assertEquals(Set(410,444,510), a4.toSet)
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

    val blockHead = DirectMemoryManager.getBlock(manager1.getBeginBlockId)
    val blockNext1 = DirectMemoryManager.getBlock(blockHead.thisBlockNextBlockId)
    val blockNext2 = DirectMemoryManager.getBlock(blockNext1.thisBlockNextBlockId)
    val blockNext3 = DirectMemoryManager.getBlock(blockNext2.thisBlockNextBlockId)

    val a1 = DirectMemoryManager.getBlockDataArray(blockHead.thisBlockId)
    val a2 = DirectMemoryManager.getBlockDataArray(blockNext1.thisBlockId)
    val a3 = DirectMemoryManager.getBlockDataArray(blockNext2.thisBlockId)
    val a4 = DirectMemoryManager.getBlockDataArray(blockNext3.thisBlockId)

    Assert.assertEquals(Set(10,20,30,40,50), a1.toSet)
    Assert.assertEquals(Set(60,70,80,90,100), a2.toSet)
    Assert.assertEquals(Set(110,210,310), a3.toSet)
    Assert.assertEquals(Set(410,444,445,510), a4.toSet)
  }

  @Test
  def testIterator(){
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

    manager1.put(333)

    val iter = manager1.getAllBlockNodeIds()
    var count = 0
    while (iter.hasNext){
      count += 1
    }
    Assert.assertEquals(16, count)
  }

  def testIsExist(): Unit ={
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

    manager1.put(333)

    Assert.assertEquals(true, manager1.isExist(110))
    Assert.assertEquals(false, manager1.isExist(1110))

  }
  @Test
  def testGetAllBlockNodeIds(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(60)
    manager1.put(70)
    val iter = manager1.getAllBlockNodeIds()
    while (iter.hasNext){
      println(iter.next())
    }
  }

  @Test
  def testOneBlockSplitAndQuery(): Unit ={
    val manager1 = new OutGoingEdgeBlockManager()

    manager1.put(10)
    manager1.put(20)
    manager1.put(30)
    manager1.put(40)
    manager1.put(50)

    manager1.put(33)
    manager1.put(34)
    manager1.put(35)
    manager1.put(36)
//    manager1.getAllBlocks().foreach(_.nodeIdArray.toSet.foreach(println))
    val iter = manager1.getAllBlockNodeIds()
    while (iter.hasNext){
      println(iter.next())
    }
  }
}
