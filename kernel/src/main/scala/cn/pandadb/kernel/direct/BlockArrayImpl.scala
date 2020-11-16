package cn.pandadb.kernel.direct

import io.netty.buffer.{ByteBuf, Unpooled}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DirectMemoryManager {
  /*
      DATA_LENGTH:    the num of a block's endNodesId
      BLOCK_SIZE:     the size of a block
                      34 = blockId + preBlockId + nextBlockId + minNodeId + maxNodeId
                      2 = arrayUsedSize
      MAX_CAPACITY:   make sure allocate integer blocks
                      2147483647 = 2GB, a directBuffer's maxSize is 2GB
   */
  val DATA_LENGTH = 5
  val BLOCK_SIZE = 34 + DATA_LENGTH * 8 + 2
  val MAX_CAPACITY = 2147483647 - (2147483647 % BLOCK_SIZE)

  val deleteLog: mutable.Queue[BlockId] = new mutable.Queue[BlockId]()
  val directBufferPageArray = new ArrayBuffer[ByteBuf]()

  private var pageId: Short = 0
  private var currentPageBlockId = 0

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

  def putBlockDataToDirectBuffer(block: EndNodesBlock): Unit = {
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
        throw new Exception("no such block...")
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
      DirectMemoryManager.putBlockDataToDirectBuffer(newBlock)
    }
    // have block
    else {
      var foundToPut = false
      var queryBlock = DirectMemoryManager.getBlock(beginBlockId)
      while (!foundToPut) {
        val isFound = queryBlockToInsertStrategy(queryBlock, nodeId)
        if (!isFound) {
          if (queryBlock.thisBlockNextBlockId != BlockId()) {
            queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
          } else {
            throw new Exception("Not found position to put...")
          }
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
      if (res._1) {
        DirectMemoryManager.updateBlockData(queryBlock)
      }
      else if (res._2) {
        if (res._3 != BlockId()) beginBlockId = res._3
        if (preBlock != null) {
          preBlock.thisBlockNextBlockId = res._4
          DirectMemoryManager.updateBlockIds(preBlock)
        }
        if (nextBlock != null) {
          nextBlock.thisBlockPreBlockId = res._5
          DirectMemoryManager.updateBlockIds(nextBlock)
        }
      }
      isFound = true
    }
    else if (queryBlock.arrayUsedSize < DirectMemoryManager.DATA_LENGTH) {
      var putResult: (Boolean, Boolean, BlockId, BlockId, BlockId) = (false, false, BlockId(), BlockId(), BlockId())
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
      else if (putResult._2) {
        if (putResult._3 != BlockId()) beginBlockId = putResult._3
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

        DirectMemoryManager.putBlockDataToDirectBuffer(newBlock)

        queryBlock.thisBlockPreBlockId = newBlock.thisBlockId
        DirectMemoryManager.updateBlockIds(queryBlock)

        beginBlockId = newBlock.thisBlockId

        isFound = true
      }
      // insert to left
      else if (preBlock != null && nodeId > preBlock.thisBlockMaxNodeId && nodeId < queryBlock.thisBlockMinNodeId) {
        //        val leftBlock = new EndNodesBlock(preBlock.thisBlockId, queryBlock.thisBlockId, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
        val leftBlock = DirectMemoryManager.generateBlock()
        leftBlock.put(nodeId)

        leftBlock.thisBlockPreBlockId = preBlock.thisBlockId
        leftBlock.thisBlockNextBlockId = queryBlock.thisBlockId
        leftBlock.thisBlockMinNodeId = nodeId
        leftBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockDataToDirectBuffer(leftBlock)

        preBlock.thisBlockNextBlockId = leftBlock.thisBlockId
        queryBlock.thisBlockPreBlockId = leftBlock.thisBlockId

        DirectMemoryManager.updateBlockIds(preBlock)
        DirectMemoryManager.updateBlockIds(queryBlock)

        isFound = true
      }
      // insert to right
      else if (nextBlock != null && nodeId < nextBlock.thisBlockMinNodeId && nodeId > queryBlock.thisBlockMaxNodeId) {
        //        val rightBlock = new EndNodesBlock(queryBlock.thisBlockId, nextBlock.thisBlockId, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
        val rightBlock = DirectMemoryManager.generateBlock()
        rightBlock.put(nodeId)

        rightBlock.thisBlockPreBlockId = queryBlock.thisBlockId
        rightBlock.thisBlockNextBlockId = nextBlock.thisBlockId
        rightBlock.thisBlockMinNodeId = nodeId
        rightBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockDataToDirectBuffer(rightBlock)

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

        DirectMemoryManager.putBlockDataToDirectBuffer(tailBlock)

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
    var isFound = true
    while (flag) {
      if (!block.isExist(endNodeId)) {
        if (block.thisBlockNextBlockId != BlockId()) {
          block = DirectMemoryManager.getBlock(block.thisBlockNextBlockId)
        }
        else {
          flag = false
          isFound = false
        }
      }
      else {
        flag = false
        isFound = false
      }
    }
    isFound
  }

  def getAll(): Iterator[EndNodesBlock] = {
    new GetAllBlockNodes(this)
  }
}

class EndNodesBlock(blockId: BlockId, preBlock: BlockId, nextBlock: BlockId,
                    minNodeId: Long, maxNodeId: Long,
                    dataLength: Int, deleteLog: mutable.Queue[BlockId]) {
  type isUpdate = Boolean
  type isSplit = Boolean

  var nodeIdArray: Array[Long] = new Array[Long](dataLength)
  var arrayUsedSize: Short = 0

  var thisBlockMinNodeId: Long = minNodeId
  var thisBlockMaxNodeId: Long = maxNodeId
  var thisBlockId: BlockId = blockId
  var thisBlockNextBlockId: BlockId = nextBlock
  var thisBlockPreBlockId: BlockId = preBlock

  def put(nodeId: Long): (isUpdate, isSplit, BlockId, BlockId, BlockId) = {
    var split = false
    var update = false
    var newHeadId: BlockId = BlockId()
    var smallerBlockId: BlockId = BlockId()
    var biggerBlockId: BlockId = BlockId()

    if (arrayUsedSize < nodeIdArray.length) {
      insertToArray(nodeId)
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

      if (nodeId < nodeIdArray(splitAt)) {
        // nodeId belong to smaller block
        for (i <- 0 until splitAt) smallerBlock.put(nodeIdArray(i))
        smallerBlock.put(nodeId)
        for (i <- splitAt until dataLength) biggerBlock.put(nodeIdArray(i))
      }
      // nodeId belong to bigger block
      else {
        for (i <- 0 to splitAt) smallerBlock.put(nodeIdArray(i))
        for (i <- (splitAt + 1) until dataLength) biggerBlock.put(nodeIdArray(i))
        biggerBlock.put(nodeId)
      }
      smallerBlock.thisBlockMinNodeId = smallerBlock.nodeIdArray(0)
      smallerBlock.thisBlockMaxNodeId = smallerBlock.nodeIdArray(splitAt)
      biggerBlock.thisBlockMinNodeId = biggerBlock.nodeIdArray(0)
      biggerBlock.thisBlockMaxNodeId = biggerBlock.nodeIdArray(splitAt)

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

      DirectMemoryManager.putBlockDataToDirectBuffer(smallerBlock)
      DirectMemoryManager.putBlockDataToDirectBuffer(biggerBlock)
      split = true
    }
    (update, split, newHeadId, smallerBlockId, biggerBlockId)
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
    if (block.thisBlockNextBlockId != BlockId()){
      id = block.thisBlockNextBlockId
    }
    block
  }
}