package cn.pandadb.kernel.direct

import cn.pandadb.kernel.direct.DirectMemoryManager.Page
import io.netty.buffer.{ByteBuf, Unpooled}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DirectMemoryManager{
  type Page = Short

  val BLOCK_LENGTH = 5
  // last single 2 is usedSize
  val BLOCK_SIZE = 2 * 3 + 4 * 3 + 8 * 2 + BLOCK_LENGTH * 8 + 2

  val MAX_CAPACITY = 2147483647 - (2147483647 % BLOCK_SIZE)

  val deleteLog: mutable.Queue[(Page, Int)] = new mutable.Queue[(Page, Int)]()
  val directBufferPageArray = new ArrayBuffer[ByteBuf]()

  var pageId: Short = -1
  var currentPageBlockId = 0

  def mallocDirectBuffer(): Unit ={
    val directBuffer = Unpooled.directBuffer(MAX_CAPACITY)

    pageId = (pageId + 1).toShort
    currentPageBlockId = 0 // reset index to new page

    directBufferPageArray.append(directBuffer)
  }

  def generateBlock(): EndNodesBlock ={
    var blockId: (Short, Int) = (-1, -1)

    if (deleteLog.nonEmpty){
      blockId = deleteLog.dequeue()
    }
    else{
      if (pageId == -1){
        mallocDirectBuffer()
      }
      else if (currentPageBlockId > MAX_CAPACITY - BLOCK_SIZE ){
        mallocDirectBuffer()
      }
      blockId = (pageId, currentPageBlockId)

      currentPageBlockId += BLOCK_SIZE
    }

    new EndNodesBlock(blockId, (-1, -1), (-1, -1), -1, -1, BLOCK_LENGTH, deleteLog)
  }

  // todo: concurrent?
  def putBlockDataToDirectBuffer(block: EndNodesBlock): Unit ={
    val directBuffer = directBufferPageArray(block.thisBlockId._1)

    var flag = false

    val writerIndex = block.thisBlockId._2
    if (writerIndex < directBuffer.writerIndex()){
      directBuffer.markWriterIndex()
      directBuffer.writerIndex(writerIndex)
      flag = true
    }

    // blockId
    directBuffer.writeShort(block.thisBlockId._1)
    directBuffer.writeInt(block.thisBlockId._2)
    // preBlockId
    directBuffer.writeShort(block.thisBlockPreBlockId._1)
    directBuffer.writeInt(block.thisBlockPreBlockId._2)
    // nextBlockId
    directBuffer.writeShort(block.thisBlockNextBlockId._1)
    directBuffer.writeInt(block.thisBlockNextBlockId._2) // [0~18)bytes
    // min max node Id
    directBuffer.writeLong(block.thisBlockMinNodeId)
    directBuffer.writeLong(block.thisBlockMaxNodeId) // [18~34)bytes
    // arrayUsedSize
    directBuffer.writeShort(block.arrayUsedSize) // [34~36)
    // endNodes Id
    for (i <- 0 until BLOCK_LENGTH){
      directBuffer.writeLong(block.nodeIdArray(i))
    }
    if (flag){
      directBuffer.resetWriterIndex()
    }
  }

  def updateBlockData(block: EndNodesBlock): Unit ={
    val directBuffer = directBufferPageArray(block.thisBlockId._1)
    directBuffer.markWriterIndex()
    directBuffer.writerIndex(block.thisBlockId._2 + 18)

    directBuffer.writeLong(block.thisBlockMinNodeId)
    directBuffer.writeLong(block.thisBlockMaxNodeId)
    directBuffer.writeShort(block.arrayUsedSize)
    block.nodeIdArray.foreach(f => directBuffer.writeLong(f))

    directBuffer.resetWriterIndex()
  }
  def updateBlockIds(block: EndNodesBlock): Unit ={
    val directBuffer = directBufferPageArray(block.thisBlockId._1)
    directBuffer.markWriterIndex()
    directBuffer.writerIndex(block.thisBlockId._2)

    directBuffer.writeShort(block.thisBlockId._1)
    directBuffer.writeInt(block.thisBlockId._2)

    directBuffer.writeShort(block.thisBlockPreBlockId._1)
    directBuffer.writeInt(block.thisBlockPreBlockId._2)

    directBuffer.writeShort(block.thisBlockNextBlockId._1)
    directBuffer.writeInt(block.thisBlockNextBlockId._2)

    directBuffer.resetWriterIndex()
  }

  def getBlock(id: (Short, Int)): EndNodesBlock ={
    val directBuffer = directBufferPageArray(id._1)
    directBuffer.markReaderIndex()
    directBuffer.readerIndex(id._2)

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

    val block = new EndNodesBlock((thisBlockPage, thisBlockId), (preBlockPage, preBlockId), (nextBlockPage, nextBlockId),
      minId, maxId, BLOCK_LENGTH, deleteLog)
    block.arrayUsedSize = arrayUsedSize
    for (i <- 0 until arrayUsedSize){
      val data = directBuffer.readLong()
      block.nodeIdArray(i) = data
    }

    directBuffer.resetReaderIndex()
    block
  }
}

class OutGoingEdgeBlockManager(initBlockId: (Short, Int) = (-1, -1)) {

  private var beginBlockId = initBlockId

  def getBeginBlockId: (Short, Int) = {
    beginBlockId
  }

  def put(nodeId: Long): (Short, Int) = {
    // no block
    if (beginBlockId == (-1, -1)) {
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
          if (queryBlock.thisBlockNextBlockId != (-1, -1)) {
            queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
          } else {
            throw new Exception("Not found position to put...")
          }
        }
        else foundToPut = true
      }
    }
    beginBlockId
  }

  def queryBlockToInsertStrategy(queryBlock: EndNodesBlock, nodeId: Long): Boolean = {
    var isFound = false
    var preBlock: EndNodesBlock = null
    var nextBlock: EndNodesBlock = null
    if (queryBlock.thisBlockPreBlockId != (-1, -1)) preBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockPreBlockId)
    if (queryBlock.thisBlockNextBlockId != (-1, -1)) nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)

    if (nodeId > queryBlock.thisBlockMinNodeId && nodeId < queryBlock.thisBlockMaxNodeId) {
      val res = queryBlock.put(nodeId)
      if (res._1){
        DirectMemoryManager.updateBlockData(queryBlock)
      }
      else if (res._2){
        if (res._3 != (-1, -1)) beginBlockId = res._3
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
    else if (queryBlock.arrayUsedSize < DirectMemoryManager.BLOCK_LENGTH) {
      var putResult: (Boolean, Boolean, (Short, Int), (Short, Int), (Short, Int)) = (false, false, (-1, -1), (-1, -1),  (-1, -1))
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
      if (putResult._1){
        DirectMemoryManager.updateBlockData(queryBlock)
      }
      else if (putResult._2){
        if (putResult._3 != (-1, -1)) beginBlockId = putResult._3
      }
    }
    else if (queryBlock.arrayUsedSize == DirectMemoryManager.BLOCK_LENGTH) {
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
        //        val tailBlock = new EndNodesBlock(queryBlock.thisBlockId, -1, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
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

  def delete(nodeId: Long): (Short, Int) = {
    var queryBlock = DirectMemoryManager.getBlock(beginBlockId)
    var flag = true
    while (flag) {
      if (nodeId >= queryBlock.thisBlockMinNodeId && nodeId <= queryBlock.thisBlockMaxNodeId) {
        val remainSize = queryBlock.delete(nodeId)
        if (remainSize == 0) {
          // delete only 1 block
          if (queryBlock.thisBlockPreBlockId == (-1, -1) && queryBlock.thisBlockNextBlockId == (-1, -1)) {
            beginBlockId = (-1, -1)
            DirectMemoryManager.updateBlockData(queryBlock)
            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
          // delete head block
          else if (queryBlock.thisBlockPreBlockId == (-1, -1)) {
            val nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
            beginBlockId = nextBlock.thisBlockId
            nextBlock.thisBlockPreBlockId = (-1, -1)

            DirectMemoryManager.updateBlockIds(nextBlock)
            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
          // delete middle block
          else if (queryBlock.thisBlockPreBlockId != (-1, -1) && queryBlock.thisBlockNextBlockId != (-1, -1)) {
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
            preBlock.thisBlockNextBlockId = (-1, -1)

            DirectMemoryManager.updateBlockIds(preBlock)
            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
        }
        else{
          DirectMemoryManager.updateBlockData(queryBlock)
        }
        flag = false
      }
      else {
        if (queryBlock.thisBlockNextBlockId != (-1, -1)) {
          queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
        } else {
          flag = false
        }
      }
    }
    beginBlockId
  }

  def queryByLink(startBlockId: (Short, Int)): Unit = {
    val startIndex = startBlockId
    var flag = true
    if (startIndex != (-1, -1)) {
      var block = DirectMemoryManager.getBlock(startIndex)
      while (flag) {
        block.nodeIdArray.foreach(println)
        if (block.thisBlockNextBlockId != (-1, -1)) {
          block = DirectMemoryManager.getBlock(block.thisBlockNextBlockId)
        }
        else flag = false
        println("===========================================")
      }
    }
  }
}

class EndNodesBlock(blockId: (Short, Int), preBlock: (Short, Int), nextBlock: (Short, Int),
                    minNodeId: Long, maxNodeId: Long,
                    blockLength: Int, deleteLog: mutable.Queue[(Page, Int)]) {
  type isUpdate = Boolean
  type isSplit = Boolean

  var nodeIdArray: Array[Long] = new Array[Long](blockLength)
  var arrayUsedSize: Short = 0

  var thisBlockMinNodeId: Long = minNodeId
  var thisBlockMaxNodeId: Long = maxNodeId
  var thisBlockId: (Short, Int) = blockId
  var thisBlockNextBlockId: (Short, Int) = nextBlock
  var thisBlockPreBlockId: (Short, Int) = preBlock

  def put(nodeId: Long): (isUpdate, isSplit, (Short, Int), (Short, Int), (Short, Int)) = {
    var split = false
    var update = false
    var newHeadId: (Short, Int) = (-1, -1)
    var smallerBlockId: (Short, Int) = (-1, -1)
    var biggerBlockId: (Short, Int) = (-1, -1)

    if (arrayUsedSize < nodeIdArray.length) {
      insertToArray(nodeId)
      if (arrayUsedSize != 1) {
        if (nodeId < thisBlockMinNodeId) thisBlockMinNodeId = nodeId
        if (nodeId > thisBlockMaxNodeId) thisBlockMaxNodeId = nodeId
        update = true
      }
      else{
        thisBlockMinNodeId = nodeId
        thisBlockMaxNodeId = nodeId
      }
    }
    // split block
    else {
      val splitAt = blockLength / 2
      val smallerBlock = DirectMemoryManager.generateBlock()
      val biggerBlock = DirectMemoryManager.generateBlock()

      if (nodeId < nodeIdArray(splitAt)) {
        // nodeId belong to smaller block
        for (i <- 0 until splitAt) smallerBlock.put(nodeIdArray(i))
        smallerBlock.put(nodeId)
        for (i <- splitAt until blockLength) biggerBlock.put(nodeIdArray(i))
      }
      // nodeId belong to bigger block
      else {
        for (i <- 0 to splitAt) smallerBlock.put(nodeIdArray(i))
        for (i <- (splitAt + 1) until blockLength) biggerBlock.put(nodeIdArray(i))
        biggerBlock.put(nodeId)
      }
      smallerBlock.thisBlockMinNodeId = smallerBlock.nodeIdArray(0)
      smallerBlock.thisBlockMaxNodeId = smallerBlock.nodeIdArray(splitAt)
      biggerBlock.thisBlockMinNodeId = biggerBlock.nodeIdArray(0)
      biggerBlock.thisBlockMaxNodeId = biggerBlock.nodeIdArray(splitAt)

      // first block split
      if (thisBlockPreBlockId == (-1, -1)) {

        smallerBlock.thisBlockNextBlockId = biggerBlock.thisBlockId
        biggerBlock.thisBlockPreBlockId = smallerBlock.thisBlockId

        if (thisBlockNextBlockId != (-1, -1)) {
          biggerBlock.thisBlockNextBlockId = thisBlockNextBlockId
        }
        newHeadId = smallerBlock.thisBlockId
      }
      // middle block split
      else if (thisBlockPreBlockId != -1 && thisBlockNextBlockId != -1) {
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