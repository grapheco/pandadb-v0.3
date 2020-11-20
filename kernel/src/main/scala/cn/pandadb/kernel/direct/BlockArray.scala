package cn.pandadb.kernel.direct

import io.netty.buffer.{ByteBuf, Unpooled}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DirectMemoryManager {
  /*
      DATA_LENGTH:    the num of a block's endNodesId
      BLOCK_SIZE:     the size of a block
                      36bytes = blockId(6) + preBlockId(6) + nextBlockId(6) + minNodeId(8) + maxNodeId(8) + blockArrayUsedSize(2)
      MAX_CAPACITY:   make sure allocate mem can contain integer blocks
                      a directBuffer's maxSize is 2GB = 2147483647
   */
  val DATA_LENGTH = 5
  val BLOCK_SIZE = 36 + DATA_LENGTH * 8
  val MAX_CAPACITY = 2147483647 - (2147483647 % BLOCK_SIZE)

  val deleteLog: mutable.Queue[BlockId] = new mutable.Queue[BlockId]()
  val directBufferPageArray = new ArrayBuffer[ByteBuf]()

  private var pageId: Short = 0 // in which directBuffer
  private var currentPageBlockId = 0 // offset of pageId's directBuffer

  private def mallocDirectBuffer(): Unit = {
    val directBuffer = Unpooled.directBuffer(MAX_CAPACITY)
    pageId = (pageId + 1).toShort
    currentPageBlockId = 0 // reset index to new page
    directBufferPageArray.append(directBuffer)
  }

  def generateBlock(): EndNodesBlock = {
    this.synchronized {
      var blockId: BlockId = BlockId()

      if (deleteLog.nonEmpty) {
        blockId = deleteLog.dequeue()
      }
      else {
        if (pageId == 0) {
          mallocDirectBuffer()
        }
        else if (currentPageBlockId > MAX_CAPACITY - BLOCK_SIZE) {
          mallocDirectBuffer()
        }
        blockId = BlockId(pageId, currentPageBlockId)

        currentPageBlockId += BLOCK_SIZE
      }
      new EndNodesBlock(blockId, BlockId(), BlockId(), 0, 0, DATA_LENGTH, deleteLog)
    }
  }

  def putBlockToDirectBuffer(block: EndNodesBlock): Unit = {
    this.synchronized {
      val directBuffer = directBufferPageArray(block.thisBlockId.pageId - 1)

      var flag = false

      val writerIndex = block.thisBlockId.offset
      if (writerIndex < directBuffer.writerIndex()) {
        directBuffer.markWriterIndex()
        directBuffer.writerIndex(writerIndex)
        flag = true
      }

      // blockId
      directBuffer.writeShort(block.thisBlockId.pageId)
      directBuffer.writeInt(block.thisBlockId.offset)
      // preBlockId
      directBuffer.writeShort(block.thisBlockPreBlockId.pageId)
      directBuffer.writeInt(block.thisBlockPreBlockId.offset)
      // nextBlockId
      directBuffer.writeShort(block.thisBlockNextBlockId.pageId)
      directBuffer.writeInt(block.thisBlockNextBlockId.offset) // [0~18)bytes
      // min max node Id
      directBuffer.writeLong(block.thisBlockMinNodeId)
      directBuffer.writeLong(block.thisBlockMaxNodeId) // [18~34)bytes
      // arrayUsedSize
      directBuffer.writeShort(block.arrayUsedSize) // [34~36)
      // endNodes Id
      for (i <- 0 until DATA_LENGTH) {
        directBuffer.writeLong(block.nodeIdArray(i))
      }
      if (flag) {
        directBuffer.resetWriterIndex()
      }
    }
  }

  def addDataToBlock(block: EndNodesBlock, nodeId: Long): Unit ={
    this.synchronized {
      val directBuffer = directBufferPageArray(block.thisBlockId.pageId - 1)
      directBuffer.markWriterIndex()
      directBuffer.writerIndex(block.thisBlockId.offset + 18)

      directBuffer.writeLong(block.thisBlockMinNodeId) // + 8
      directBuffer.writeLong(block.thisBlockMaxNodeId) // + 8
      directBuffer.writeShort(block.arrayUsedSize) // + 2, now is 18 + 16 + 2 = 36

      directBuffer.writerIndex(block.thisBlockId.offset + 36 + (block.arrayUsedSize - 1) * 8)
      directBuffer.writeLong(nodeId)

      directBuffer.resetWriterIndex()
    }
  }

  def updateBlockData(block: EndNodesBlock): Unit = {
    this.synchronized {
      val directBuffer = directBufferPageArray(block.thisBlockId.pageId - 1)
      directBuffer.markWriterIndex()
      directBuffer.writerIndex(block.thisBlockId.offset + 18)

      directBuffer.writeLong(block.thisBlockMinNodeId)
      directBuffer.writeLong(block.thisBlockMaxNodeId)
      directBuffer.writeShort(block.arrayUsedSize)
      block.nodeIdArray.foreach(f => directBuffer.writeLong(f))

      directBuffer.resetWriterIndex()
    }
  }

  def updateBlockIds(block: EndNodesBlock): Unit = {
    this.synchronized {
      val directBuffer = directBufferPageArray(block.thisBlockId.pageId - 1)
      directBuffer.markWriterIndex()
      directBuffer.writerIndex(block.thisBlockId.offset)

      directBuffer.writeShort(block.thisBlockId.pageId)
      directBuffer.writeInt(block.thisBlockId.offset)

      directBuffer.writeShort(block.thisBlockPreBlockId.pageId)
      directBuffer.writeInt(block.thisBlockPreBlockId.offset)

      directBuffer.writeShort(block.thisBlockNextBlockId.pageId)
      directBuffer.writeInt(block.thisBlockNextBlockId.offset)

      directBuffer.resetWriterIndex()
    }
  }

  def getBlock(id: BlockId): EndNodesBlock = {
    this.synchronized {
      if (id == BlockId()) {
        throw new NoBlockToGetException
      }
      else {
        val directBuffer = directBufferPageArray(id.pageId - 1)
        directBuffer.markReaderIndex()
        directBuffer.readerIndex(id.offset)

        //this
        val thisBlockPage = directBuffer.readShort()
        val thisBlockId = directBuffer.readInt()
        //pre
        val preBlockPage = directBuffer.readShort()
        val preBlockId = directBuffer.readInt()
        //next
        val nextBlockPage = directBuffer.readShort()
        val nextBlockId = directBuffer.readInt()
        //range
        val minId = directBuffer.readLong()
        val maxId = directBuffer.readLong()
        // used size
        val arrayUsedSize = directBuffer.readShort()

        val block = new EndNodesBlock(BlockId(thisBlockPage, thisBlockId), BlockId(preBlockPage, preBlockId), BlockId(nextBlockPage, nextBlockId),
          minId, maxId, DATA_LENGTH, deleteLog)
        block.arrayUsedSize = arrayUsedSize
        for (i <- 0 until arrayUsedSize) {
          val data = directBuffer.readLong()
          block.nodeIdArray(i) = data
        }

        directBuffer.resetReaderIndex()
        block
      }
    }
  }
}

class OutGoingEdgeBlockManager(initBlockId: BlockId = BlockId()) {

  private var beginBlockId = initBlockId

  def getBeginBlockId: BlockId = {
    beginBlockId
  }

  def put(nodeId: Long): Boolean = {
    // no block
    if (beginBlockId == BlockId()) {
      val newBlock = DirectMemoryManager.generateBlock()
      newBlock.put(nodeId)
      beginBlockId = newBlock.thisBlockId
      DirectMemoryManager.putBlockToDirectBuffer(newBlock)
    }
    // have block
    else {
      var foundToPut = false
      var queryBlock = DirectMemoryManager.getBlock(beginBlockId)
      while (!foundToPut) {
        val isFound = queryBlockToInsertStrategy(queryBlock, nodeId)
        if (!isFound) {
          queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
        }
        else foundToPut = true
      }
    }
    true
  }

  private def queryBlockToInsertStrategy(queryBlock: EndNodesBlock, nodeId: Long): Boolean = {
    var isFound = false
    var preBlock: EndNodesBlock = null
    var nextBlock: EndNodesBlock = null
    if (queryBlock.thisBlockPreBlockId != BlockId()) preBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockPreBlockId)
    if (queryBlock.thisBlockNextBlockId != BlockId()) nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)

    if (nodeId > queryBlock.thisBlockMinNodeId && nodeId < queryBlock.thisBlockMaxNodeId) {
      val res = queryBlock.put(nodeId)
      // if isUpdate, it means block not full, so we just update it's data
      if (res._1) {
//        DirectMemoryManager.updateBlockData(queryBlock)
        DirectMemoryManager.addDataToBlock(queryBlock, nodeId)
      }
      else {
        if (res._2 != BlockId()) beginBlockId = res._2 // means pre = null, and change head
        if (preBlock != null) {
          preBlock.thisBlockNextBlockId = res._3
          DirectMemoryManager.updateBlockIds(preBlock)
        }
        if (nextBlock != null) {
          nextBlock.thisBlockPreBlockId = res._4
          DirectMemoryManager.updateBlockIds(nextBlock)
        }
      }
      isFound = true
    }
    else if (queryBlock.arrayUsedSize < DirectMemoryManager.DATA_LENGTH) {
      var putResult: (Boolean, BlockId, BlockId, BlockId) = (false, BlockId(), BlockId(), BlockId())
      // put to this block depends on preBlock max and nextBlock min
      if (preBlock == null && nextBlock == null) {
        putResult = queryBlock.put(nodeId)
        isFound = true
      }
      else if (preBlock == null && nextBlock != null) {
        if (nodeId < nextBlock.thisBlockMinNodeId) {
          putResult = queryBlock.put(nodeId)
          isFound = true
        }
      }
      else if (preBlock != null && nextBlock == null) {
        if (nodeId > preBlock.thisBlockMaxNodeId) {
          putResult = queryBlock.put(nodeId)
          isFound = true
        }
      }
      else {
        if (nodeId > preBlock.thisBlockMaxNodeId && nodeId < nextBlock.thisBlockMinNodeId) {
          putResult = queryBlock.put(nodeId)
          isFound = true
        }
      }
      // update result
      if (putResult._1) {
        DirectMemoryManager.updateBlockData(queryBlock)
      }
    }
    else if (queryBlock.arrayUsedSize == DirectMemoryManager.DATA_LENGTH) {
      // new head
      if (preBlock == null && nodeId < queryBlock.thisBlockMinNodeId) {
        val newBlock = DirectMemoryManager.generateBlock()
        newBlock.put(nodeId)

        newBlock.thisBlockNextBlockId = queryBlock.thisBlockId
        newBlock.thisBlockMinNodeId = nodeId
        newBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockToDirectBuffer(newBlock)

        queryBlock.thisBlockPreBlockId = newBlock.thisBlockId
        DirectMemoryManager.updateBlockIds(queryBlock)

        beginBlockId = newBlock.thisBlockId

        isFound = true
      }
      // insert to left
      else if (preBlock != null && nodeId > preBlock.thisBlockMaxNodeId && nodeId < queryBlock.thisBlockMinNodeId) {
        val leftBlock = DirectMemoryManager.generateBlock()
        leftBlock.put(nodeId)

        leftBlock.thisBlockPreBlockId = preBlock.thisBlockId
        leftBlock.thisBlockNextBlockId = queryBlock.thisBlockId
        leftBlock.thisBlockMinNodeId = nodeId
        leftBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockToDirectBuffer(leftBlock)

        preBlock.thisBlockNextBlockId = leftBlock.thisBlockId
        queryBlock.thisBlockPreBlockId = leftBlock.thisBlockId

        DirectMemoryManager.updateBlockIds(preBlock)
        DirectMemoryManager.updateBlockIds(queryBlock)

        isFound = true
      }
      // insert to right
      else if (nextBlock != null && nodeId < nextBlock.thisBlockMinNodeId && nodeId > queryBlock.thisBlockMaxNodeId) {
        val rightBlock = DirectMemoryManager.generateBlock()
        rightBlock.put(nodeId)

        rightBlock.thisBlockPreBlockId = queryBlock.thisBlockId
        rightBlock.thisBlockNextBlockId = nextBlock.thisBlockId
        rightBlock.thisBlockMinNodeId = nodeId
        rightBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockToDirectBuffer(rightBlock)

        queryBlock.thisBlockNextBlockId = rightBlock.thisBlockId
        nextBlock.thisBlockPreBlockId = rightBlock.thisBlockId

        DirectMemoryManager.updateBlockIds(queryBlock)
        DirectMemoryManager.updateBlockIds(nextBlock)

        isFound = true
      }
      // new tail
      else if (nextBlock == null && nodeId > queryBlock.thisBlockMaxNodeId) {
        val tailBlock = DirectMemoryManager.generateBlock()
        tailBlock.put(nodeId)

        tailBlock.thisBlockPreBlockId = queryBlock.thisBlockId
        tailBlock.thisBlockMinNodeId = nodeId
        tailBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockToDirectBuffer(tailBlock)

        queryBlock.thisBlockNextBlockId = tailBlock.thisBlockId
        DirectMemoryManager.updateBlockIds(queryBlock)

        isFound = true
      }
    }
    isFound
  }

  def delete(nodeId: Long): Boolean = {
    var queryBlock = DirectMemoryManager.getBlock(beginBlockId)
    var flag = true
    var isFound = true
    while (flag) {
      if (nodeId >= queryBlock.thisBlockMinNodeId && nodeId <= queryBlock.thisBlockMaxNodeId) {
        val remainSize = queryBlock.delete(nodeId)
        if (remainSize == 0) {
          // delete only 1 block
          if (queryBlock.thisBlockPreBlockId == BlockId() && queryBlock.thisBlockNextBlockId == BlockId()) {
            beginBlockId = BlockId()
            DirectMemoryManager.updateBlockData(queryBlock)
            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
          // delete head block
          else if (queryBlock.thisBlockPreBlockId == BlockId()) {
            val nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
            beginBlockId = nextBlock.thisBlockId
            nextBlock.thisBlockPreBlockId = BlockId()

            DirectMemoryManager.updateBlockIds(nextBlock)
            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
          // delete middle block
          else if (queryBlock.thisBlockPreBlockId != BlockId() && queryBlock.thisBlockNextBlockId != BlockId()) {
            val preBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockPreBlockId)
            val nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
            preBlock.thisBlockNextBlockId = nextBlock.thisBlockId
            nextBlock.thisBlockPreBlockId = preBlock.thisBlockId

            DirectMemoryManager.updateBlockIds(preBlock)
            DirectMemoryManager.updateBlockIds(nextBlock)
            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
          // delete last block
          else {
            val preBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockPreBlockId)
            preBlock.thisBlockNextBlockId = BlockId()

            DirectMemoryManager.updateBlockIds(preBlock)
            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
        }
        else {
          DirectMemoryManager.updateBlockData(queryBlock)
        }
        flag = false
      }
      else {
        if (queryBlock.thisBlockNextBlockId != BlockId()) {
          queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
        } else {
          flag = false
          isFound = false
        }
      }
    }
    isFound
  }

  def isExist(endNodeId: Int): Boolean = {
    var block = DirectMemoryManager.getBlock(beginBlockId)
    var flag = true
    var isFound = false
    while (flag) {
      val res = block.isExist(endNodeId)
      if (!res) {
        if (block.thisBlockNextBlockId != BlockId()) {
          block = DirectMemoryManager.getBlock(block.thisBlockNextBlockId)
        }
        else flag = false
      }
      else {
        isFound = true
        flag = false
      }
    }
    isFound
  }

  def getAllBlocks(): Iterator[EndNodesBlock] = {
    new GetAllBlockNodes(this)
  }
  def getAllBlockNodeIds(): Iterator[Long] = {
    new GetAllBlockNodesId(this)
  }
}

class EndNodesBlock(blockId: BlockId, preBlock: BlockId, nextBlock: BlockId,
                    minNodeId: Long, maxNodeId: Long,
                    dataLength: Int, deleteLog: mutable.Queue[BlockId]) {
  type isUpdate = Boolean

  var nodeIdArray: Array[Long] = new Array[Long](dataLength)
  var arrayUsedSize: Short = 0

  var thisBlockMinNodeId: Long = minNodeId
  var thisBlockMaxNodeId: Long = maxNodeId
  var thisBlockId: BlockId = blockId
  var thisBlockNextBlockId: BlockId = nextBlock
  var thisBlockPreBlockId: BlockId = preBlock

  def put(nodeId: Long): (isUpdate, BlockId, BlockId, BlockId) = {
    var update = false
    var newHeadId: BlockId = BlockId()
    var smallerBlockId: BlockId = BlockId()
    var biggerBlockId: BlockId = BlockId()

    if (arrayUsedSize < nodeIdArray.length) {
//      insertToArray(nodeId)
      nodeIdArray(arrayUsedSize) = nodeId
      arrayUsedSize = (arrayUsedSize + 1).toShort
      if (arrayUsedSize != 1) {
        if (nodeId < thisBlockMinNodeId) thisBlockMinNodeId = nodeId
        if (nodeId > thisBlockMaxNodeId) thisBlockMaxNodeId = nodeId
        update = true
      }
      else {
        thisBlockMinNodeId = nodeId
        thisBlockMaxNodeId = nodeId
      }
    }
    // split block
    else {
      val splitAt = dataLength / 2
      val smallerBlock = DirectMemoryManager.generateBlock()
      val biggerBlock = DirectMemoryManager.generateBlock()

      val tmpArray = nodeIdArray.sorted
      if (nodeId < tmpArray(splitAt)) {
        // nodeId belong to smaller block
        for (i <- 0 until splitAt) smallerBlock.put(tmpArray(i))
        smallerBlock.put(nodeId)
        for (i <- splitAt until dataLength) biggerBlock.put(tmpArray(i))
      }
      // nodeId belong to bigger block
      else {
        for (i <- 0 to splitAt) smallerBlock.put(tmpArray(i))
        for (i <- (splitAt + 1) until dataLength) biggerBlock.put(tmpArray(i))
        biggerBlock.put(nodeId)
      }
      val tmpA = smallerBlock.nodeIdArray.slice(0, splitAt + 1).sorted
      val tmpB = biggerBlock.nodeIdArray.slice(0, splitAt + 1).sorted

      smallerBlock.thisBlockMinNodeId = tmpA(0)
      smallerBlock.thisBlockMaxNodeId = tmpA(splitAt)
      biggerBlock.thisBlockMinNodeId = tmpB(0)
      biggerBlock.thisBlockMaxNodeId = tmpB(splitAt)

      // first block split
      if (thisBlockPreBlockId == BlockId()) {

        smallerBlock.thisBlockNextBlockId = biggerBlock.thisBlockId
        biggerBlock.thisBlockPreBlockId = smallerBlock.thisBlockId

        if (thisBlockNextBlockId != BlockId()) {
          biggerBlock.thisBlockNextBlockId = thisBlockNextBlockId
        }
        newHeadId = smallerBlock.thisBlockId
      }
      // middle block split
      else if (thisBlockPreBlockId != BlockId() && thisBlockNextBlockId != BlockId()) {
        smallerBlock.thisBlockPreBlockId = thisBlockPreBlockId
        smallerBlock.thisBlockNextBlockId = biggerBlock.thisBlockId

        biggerBlock.thisBlockPreBlockId = smallerBlock.thisBlockId
        biggerBlock.thisBlockNextBlockId = thisBlockNextBlockId
      }
      // last block split
      else {
        smallerBlock.thisBlockPreBlockId = thisBlockPreBlockId
        smallerBlock.thisBlockNextBlockId = biggerBlock.thisBlockId

        biggerBlock.thisBlockPreBlockId = smallerBlock.thisBlockId
      }
      deleteLog.enqueue(thisBlockId)

      smallerBlockId = smallerBlock.thisBlockId
      biggerBlockId = biggerBlock.thisBlockId

      DirectMemoryManager.putBlockToDirectBuffer(smallerBlock)
      DirectMemoryManager.putBlockToDirectBuffer(biggerBlock)
    }
    (update, newHeadId, smallerBlockId, biggerBlockId)
  }

  def isExist(endNodeId: Long): Boolean = {
    val res = nodeIdArray.find(p => p == endNodeId)
    res.isDefined
  }

  def delete(nodeId: Long): Int = {
    for (i <- 0 until arrayUsedSize) {
      if (nodeIdArray(i) == nodeId) {
        arrayUsedSize = (arrayUsedSize - 1).toShort
        if (arrayUsedSize != 0) {
          // resort
          for (j <- i until arrayUsedSize) {
            nodeIdArray(j) = nodeIdArray(j + 1)
          }
          nodeIdArray(arrayUsedSize) = 0
          thisBlockMinNodeId = nodeIdArray(0)
          thisBlockMaxNodeId = nodeIdArray(arrayUsedSize - 1)
        }
      }
    }
    arrayUsedSize
  }

  def insertToArray(nodeId: Long): Unit = {
    nodeIdArray(arrayUsedSize) = nodeId
    arrayUsedSize = (arrayUsedSize + 1).toShort

    if (arrayUsedSize > 1) {
      for (i <- Range(1, arrayUsedSize, 1)) {
        var j = i - 1
        val tmp = nodeIdArray(i)
        while (j >= 0 && nodeIdArray(j) > tmp) {
          nodeIdArray(j + 1) = nodeIdArray(j)
          j -= 1
        }
        nodeIdArray(j + 1) = tmp
      }
    }
  }
}

// pageId: in which directBuffer
case class BlockId(pageId: Short = 0, offset: Int = 0) {}

class GetAllBlockNodes(manager: OutGoingEdgeBlockManager) extends Iterator[EndNodesBlock] {
  var blockId = manager.getBeginBlockId
  var isHead = true

  var id: BlockId = manager.getBeginBlockId

  override def hasNext: Boolean = {
    if (isHead) {
      if (manager.getBeginBlockId != BlockId()) {
        isHead = false
        true
      }
      else false
    }
    else {
      val block = DirectMemoryManager.getBlock(blockId)
      if (block.thisBlockNextBlockId != BlockId()) {
        blockId = block.thisBlockNextBlockId
        true
      }
      else {
        false
      }
    }
  }

  override def next(): EndNodesBlock = {
    val block = DirectMemoryManager.getBlock(id)
    if (block.thisBlockNextBlockId != BlockId()) {
      id = block.thisBlockNextBlockId
    }
    block
  }
}

class GetAllBlockNodesId(manager: OutGoingEdgeBlockManager) extends Iterator[Long] {
  var block: EndNodesBlock = _
  var nextBlockId: BlockId = _
  var arrayUsedSize:Short = _
  var count = 0
  var isFinish = false

  if (manager.getBeginBlockId != BlockId()){
    block = DirectMemoryManager.getBlock(manager.getBeginBlockId)
    nextBlockId = block.thisBlockNextBlockId
    arrayUsedSize = block.arrayUsedSize
  }

  override def hasNext: Boolean = {
    if (manager.getBeginBlockId == BlockId()){
      isFinish = true
      false
    }
    else if (count < arrayUsedSize) {
      count += 1
      true
    }
    else {
      if (nextBlockId != BlockId()){
        block = DirectMemoryManager.getBlock(nextBlockId)
        nextBlockId = block.thisBlockNextBlockId
        arrayUsedSize = block.arrayUsedSize
        count = 1
        true
      }
      else{
        isFinish = true
        false
      }
    }
  }

  override def next(): Long = {
    if (!isFinish) block.nodeIdArray(count - 1)
    else throw new NoNextNodeIdException
  }
}

class NoBlockToGetException extends Exception {
  override def getMessage: String = "No such block to get"
}

class NoNextNodeIdException extends Exception{
  override def getMessage: String = "next on empty iterator"

}