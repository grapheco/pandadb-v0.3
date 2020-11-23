package cn.pandadb.kernel.direct

import io.netty.buffer.{ByteBuf, Unpooled}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object UpdateBlockPosition extends Enumeration {
  val PRE = Value("1")
  val NEXT = Value("2")
}

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

  //
  def putInitBlockToDirectBuffer(block: EndNodesBlock, nodeIdArray: Array[Long], isSplit:Boolean): Unit = {
    val directBuffer = directBufferPageArray(block.thisBlockId.pageId - 1)

    // blockId
    directBuffer.setShort(block.thisBlockId.offset, block.thisBlockId.pageId)
    directBuffer.setInt(block.thisBlockId.offset + 2, block.thisBlockId.offset)
    // preBlockId
    directBuffer.setShort(block.thisBlockId.offset + 6, block.thisBlockPreBlockId.pageId)
    directBuffer.setInt(block.thisBlockId.offset + 8, block.thisBlockPreBlockId.offset)
    // nextBlockId
    directBuffer.setShort(block.thisBlockId.offset + 12, block.thisBlockNextBlockId.pageId)
    directBuffer.setInt(block.thisBlockId.offset + 14, block.thisBlockNextBlockId.offset)
    // min max node Id
    directBuffer.setLong(block.thisBlockId.offset + 18, block.thisBlockMinNodeId)
    directBuffer.setLong(block.thisBlockId.offset + 26, block.thisBlockMaxNodeId)
    // arrayUsedSize
    directBuffer.setShort(block.thisBlockId.offset + 34, block.arrayUsedSize)

    // endNodes Id
    if (!isSplit){
      directBuffer.setLong(block.thisBlockId.offset + 36, nodeIdArray(0))
    }else{
      for (i <- nodeIdArray.indices){
        directBuffer.setLong(block.thisBlockId.offset + 36 + i * 8, nodeIdArray(i))
      }
    }
  }
  def addNodeIdToBlock(block: EndNodesBlock, nodeId: Long, minChanged:Boolean, maxChanged:Boolean): Unit = {
    val directBuffer = directBufferPageArray(block.thisBlockId.pageId - 1)

    if (minChanged){
      directBuffer.setLong(block.thisBlockId.offset + 18, block.thisBlockMinNodeId)
      directBuffer.setShort(block.thisBlockId.offset + 34, block.arrayUsedSize)
    }
    else if (maxChanged){
      directBuffer.setLong(block.thisBlockId.offset + 26, block.thisBlockMaxNodeId)
      directBuffer.setShort(block.thisBlockId.offset + 34, block.arrayUsedSize)
    }
    else {
      directBuffer.setShort(block.thisBlockId.offset + 34, block.arrayUsedSize)
    }
    directBuffer.setLong(block.thisBlockId.offset + 36 + (block.arrayUsedSize - 1) * 8, nodeId)
  }
  def updateBlockIds(blockId: BlockId, position: UpdateBlockPosition.Value , value:BlockId): Unit = {
    val directBuffer = directBufferPageArray(blockId.pageId - 1)

    position match {
      case UpdateBlockPosition.PRE =>{
        directBuffer.setShort(blockId.offset + 6, value.pageId)
        directBuffer.setInt(blockId.offset + 8, value.offset)
      }
      case UpdateBlockPosition.NEXT =>{
        directBuffer.setShort(blockId.offset + 12, value.pageId)
        directBuffer.setInt(blockId.offset + 14, value.offset)
      }
    }
  }
  def getBlockInfo(id: BlockId): BlockInfo = {
    if (id == BlockId()) {
      throw new NoBlockToGetException
    }
    else {
      val directBuffer = directBufferPageArray(id.pageId - 1)

      //this
      val thisBlockPage = directBuffer.getShort(id.offset)
      val thisBlockId = directBuffer.getInt(id.offset + 2)
      //pre
      val preBlockPage = directBuffer.getShort(id.offset + 6)
      val preBlockId = directBuffer.getInt(id.offset + 8)
      //next
      val nextBlockPage = directBuffer.getShort(id.offset + 12)
      val nextBlockId = directBuffer.getInt(id.offset + 14)
      //range
      val minId = directBuffer.getLong(id.offset + 18)
      val maxId = directBuffer.getLong(id.offset + 26)
      // used size
      val arrayUsedSize = directBuffer.getShort(id.offset + 34)

      val blockInfo = BlockInfo(BlockId(thisBlockPage, thisBlockId), BlockId(preBlockPage, preBlockId),
        BlockId(nextBlockPage, nextBlockId), minId, maxId, arrayUsedSize)
      blockInfo
    }
  }
  def getBlock(id: BlockId): EndNodesBlock = {
    if (id == BlockId()) {
      throw new NoBlockToGetException
    }
    else {
      val directBuffer = directBufferPageArray(id.pageId - 1)

      //this
      val thisBlockPage = directBuffer.getShort(id.offset)
      val thisBlockId = directBuffer.getInt(id.offset + 2)
      //pre
      val preBlockPage = directBuffer.getShort(id.offset + 6)
      val preBlockId = directBuffer.getInt(id.offset + 8)
      //next
      val nextBlockPage = directBuffer.getShort(id.offset + 12)
      val nextBlockId = directBuffer.getInt(id.offset + 14)
      //range
      val minId = directBuffer.getLong(id.offset + 18)
      val maxId = directBuffer.getLong(id.offset + 26)
      // used size
      val arrayUsedSize = directBuffer.getShort(id.offset + 34)

      val block = new EndNodesBlock(BlockId(thisBlockPage, thisBlockId), BlockId(preBlockPage, preBlockId), BlockId(nextBlockPage, nextBlockId), minId, maxId, DATA_LENGTH, deleteLog)
      block.arrayUsedSize = arrayUsedSize
      block
    }
  }
  def queryBlockData(id: BlockId): BlockId ={
    var nextId: BlockId = null
    val directBuffer = directBufferPageArray(id.pageId - 1)
    //next
    val nextBlockPage = directBuffer.getShort(id.offset + 12)
    val nextBlockId = directBuffer.getInt(id.offset + 14)
    nextId = BlockId(nextBlockPage, nextBlockId)

    val arrayUsedSize = directBuffer.getShort(id.offset + 34)
    for (i <- 1 to arrayUsedSize){
      println(directBuffer.getLong(id.offset + 36 + (i-1) * 8))
    }
    nextId
  }
  def getBlockDataArray(id: BlockId): ArrayBuffer[Long] ={
    var dataArray: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    if (id == BlockId()) {
      throw new NoBlockToGetException
    }
    else {
      val directBuffer = directBufferPageArray(id.pageId - 1)
      val arrayUsedSize = directBuffer.getShort(id.offset + 34)
      for (i <- 0 until arrayUsedSize){
        dataArray += directBuffer.getLong(id.offset + 36 + i * 8 )
      }
    }
    dataArray
  }
  def updateBlockData(id: BlockId, dataArray: Array[Long]): Unit ={
    val directBuffer = directBufferPageArray(id.pageId - 1)
    // min max node Id
    directBuffer.setLong(id.offset + 18, dataArray.head)
    directBuffer.setLong(id.offset + 26, dataArray.last)
    directBuffer.setShort(id.offset + 34, dataArray.length)
    for (i <- dataArray.indices){
      directBuffer.setLong(id.offset + 36 + i * 8, dataArray(i))
    }
  }
  /////
}

class OutGoingEdgeBlockManager(initBlockId: BlockId = BlockId()) {

  private var beginBlockId = initBlockId

  def getBeginBlockId: BlockId = {
    beginBlockId
  }

  def put(nodeId: Long): BlockId = {
    // no block
    if (beginBlockId == BlockId()) {
      val newBlock = DirectMemoryManager.generateBlock()
      beginBlockId = newBlock.thisBlockId
      newBlock.put(nodeId)
    }
    // have block
    else {
      var isFinished = false
      var queryBlock = DirectMemoryManager.getBlock(beginBlockId)
      while (!isFinished) {
        val res = queryBlockToPutStrategy(queryBlock, nodeId)
        if (!res._1) {
          queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
        }
        else {
          isFinished = true
          if (res._2 != BlockId()) beginBlockId = res._2
        }
      }
    }
    beginBlockId
  }

  private def queryBlockToPutStrategy(queryBlock: EndNodesBlock, nodeId: Long): (Boolean, BlockId) = {
    var isFound = false
    var newHeadId = BlockId()
    var preBlockInfo: BlockInfo = null
    var nextBlockInfo: BlockInfo = null
    if (queryBlock.thisBlockPreBlockId != BlockId()) preBlockInfo = DirectMemoryManager.getBlockInfo(queryBlock.thisBlockPreBlockId)
    if (queryBlock.thisBlockNextBlockId != BlockId()) nextBlockInfo = DirectMemoryManager.getBlockInfo(queryBlock.thisBlockNextBlockId)

    if (nodeId > queryBlock.thisBlockMinNodeId && nodeId < queryBlock.thisBlockMaxNodeId) {
      // if isUpdate, it means block not full, so we just update it's data
      newHeadId = queryBlock.put(nodeId)
      isFound = true
    }
    else if (queryBlock.arrayUsedSize < DirectMemoryManager.DATA_LENGTH) {
      // put to this block depends on preBlock max and nextBlock min
      if (preBlockInfo == null && nextBlockInfo == null) {
        newHeadId = queryBlock.put(nodeId)
        isFound = true
      }
      else if (preBlockInfo == null && nextBlockInfo != null) {
        if (nodeId < nextBlockInfo.minNodeId) {
          newHeadId = queryBlock.put(nodeId)
          isFound = true
        }
      }
      else if (preBlockInfo != null && nextBlockInfo == null) {
        if (nodeId > preBlockInfo.maxNodeId) {
          newHeadId = queryBlock.put(nodeId)
          isFound = true
        }
      }
      else {
        if (nodeId > preBlockInfo.maxNodeId && nodeId < nextBlockInfo.minNodeId) {
          newHeadId = queryBlock.put(nodeId)
          isFound = true
        }
      }
    }
    else if (queryBlock.arrayUsedSize == DirectMemoryManager.DATA_LENGTH) {
      // new head
      if (preBlockInfo == null && nodeId < queryBlock.thisBlockMinNodeId) {
        val newBlock = DirectMemoryManager.generateBlock()
        newBlock.thisBlockNextBlockId = queryBlock.thisBlockId
        newHeadId = newBlock.put(nodeId)
        DirectMemoryManager.updateBlockIds(queryBlock.thisBlockId, UpdateBlockPosition.PRE, newBlock.thisBlockId)
        beginBlockId = newBlock.thisBlockId
        isFound = true
      }
      // insert to left
      else if (preBlockInfo != null && nodeId > preBlockInfo.maxNodeId && nodeId < queryBlock.thisBlockMinNodeId) {
        val leftBlock = DirectMemoryManager.generateBlock()
        leftBlock.thisBlockPreBlockId = preBlockInfo.blockId
        leftBlock.thisBlockNextBlockId = queryBlock.thisBlockId
        leftBlock.put(nodeId)

        DirectMemoryManager.updateBlockIds(preBlockInfo.blockId, UpdateBlockPosition.NEXT, leftBlock.thisBlockId)
        DirectMemoryManager.updateBlockIds(queryBlock.thisBlockId, UpdateBlockPosition.PRE, leftBlock.thisBlockId)

        isFound = true
      }
      // insert to right
      else if (nextBlockInfo != null && nodeId < nextBlockInfo.minNodeId && nodeId > queryBlock.thisBlockMaxNodeId) {
        val rightBlock = DirectMemoryManager.generateBlock()
        rightBlock.thisBlockPreBlockId = queryBlock.thisBlockId
        rightBlock.thisBlockNextBlockId = nextBlockInfo.blockId
        rightBlock.put(nodeId)

        DirectMemoryManager.updateBlockIds(nextBlockInfo.blockId, UpdateBlockPosition.PRE, rightBlock.thisBlockId)
        DirectMemoryManager.updateBlockIds(queryBlock.thisBlockId, UpdateBlockPosition.NEXT, rightBlock.thisBlockId)
        isFound = true
      }
      // new tail
      else if (nextBlockInfo == null && nodeId > queryBlock.thisBlockMaxNodeId) {
        val tailBlock = DirectMemoryManager.generateBlock()
        tailBlock.thisBlockPreBlockId = queryBlock.thisBlockId
        tailBlock.put(nodeId)

        DirectMemoryManager.updateBlockIds(queryBlock.thisBlockId, UpdateBlockPosition.NEXT, tailBlock.thisBlockId)


        isFound = true
      }
    }
    (isFound, newHeadId)
  }

  def delete(nodeId: Long): Unit = {
    var queryBlock = DirectMemoryManager.getBlock(beginBlockId)
    var isFinish = false
    while (!isFinish) {
      if (nodeId >= queryBlock.thisBlockMinNodeId && nodeId <= queryBlock.thisBlockMaxNodeId) {
        val remainSize = queryBlock.delete(nodeId)
        if (remainSize == 0) {
          // delete only 1 block
          if (queryBlock.thisBlockPreBlockId == BlockId() && queryBlock.thisBlockNextBlockId == BlockId()) {
            beginBlockId = BlockId()
          }
          // delete head block
          else if (queryBlock.thisBlockPreBlockId == BlockId()) {
            val nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
            beginBlockId = nextBlock.thisBlockId
            DirectMemoryManager.updateBlockIds(nextBlock.thisBlockId, UpdateBlockPosition.PRE, BlockId())
          }
          // delete middle block
          else if (queryBlock.thisBlockPreBlockId != BlockId() && queryBlock.thisBlockNextBlockId != BlockId()) {
            DirectMemoryManager.updateBlockIds(queryBlock.thisBlockPreBlockId, UpdateBlockPosition.NEXT, queryBlock.thisBlockNextBlockId)
            DirectMemoryManager.updateBlockIds(queryBlock.thisBlockNextBlockId, UpdateBlockPosition.PRE, queryBlock.thisBlockPreBlockId)
          }
          // delete last block
          else {
            DirectMemoryManager.updateBlockIds(queryBlock.thisBlockPreBlockId, UpdateBlockPosition.NEXT, BlockId())
          }
          DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
        }
        isFinish = true
      }
      else {
        if (queryBlock.thisBlockNextBlockId != BlockId()) {
          queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
        } else {
          isFinish = true
        }
      }
    }
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

  def getAllBlockNodeIds(): Iterator[Long] = {
    new GetAllBlockNodesId(this)
  }
}

class EndNodesBlock(blockId: BlockId, preBlock: BlockId, nextBlock: BlockId,
                    minNodeId: Long, maxNodeId: Long,
                    dataLength: Int, deleteLog: mutable.Queue[BlockId]) {

  var arrayUsedSize: Short = 0

  var thisBlockMinNodeId: Long = minNodeId
  var thisBlockMaxNodeId: Long = maxNodeId
  var thisBlockId: BlockId = blockId
  var thisBlockNextBlockId: BlockId = nextBlock
  var thisBlockPreBlockId: BlockId = preBlock

  def put(nodeId: Long): BlockId = {
    var newHeadId = BlockId()
    if (arrayUsedSize < DirectMemoryManager.DATA_LENGTH) {
      arrayUsedSize = (arrayUsedSize + 1).toShort
      if (arrayUsedSize == 1) {
        thisBlockMinNodeId = nodeId
        thisBlockMaxNodeId = nodeId
        DirectMemoryManager.putInitBlockToDirectBuffer(this, Array[Long](nodeId), false)
      }
      else {
        var minChanged: Boolean = false
        var maxChanged: Boolean = false
        if (nodeId < thisBlockMinNodeId) {
          thisBlockMinNodeId = nodeId
          minChanged = true
        }
        else if (nodeId > thisBlockMaxNodeId) {
          thisBlockMaxNodeId = nodeId
          maxChanged = true
        }
        DirectMemoryManager.addNodeIdToBlock(this, nodeId, minChanged, maxChanged)
      }
    }
    // split block
    else {
      val smallerBlock = DirectMemoryManager.generateBlock()
      val biggerBlock = DirectMemoryManager.generateBlock()

      var tmpArray = DirectMemoryManager.getBlockDataArray(blockId)
      tmpArray += nodeId
      tmpArray = tmpArray.sorted
      val length = tmpArray.length
      val smallerArray = tmpArray.slice(0, length/2)
      val biggerArray = tmpArray.slice(length/2, length)

      smallerBlock.thisBlockMinNodeId = smallerArray.head
      smallerBlock.thisBlockMaxNodeId = smallerArray.last
      smallerBlock.arrayUsedSize = smallerArray.length.toShort
      biggerBlock.thisBlockMinNodeId = biggerArray.head
      biggerBlock.thisBlockMaxNodeId = biggerArray.last
      biggerBlock.arrayUsedSize = biggerArray.length.toShort

      // first block split
      if (thisBlockPreBlockId == BlockId()) {
        smallerBlock.thisBlockNextBlockId = biggerBlock.thisBlockId
        biggerBlock.thisBlockPreBlockId = smallerBlock.thisBlockId
        if (thisBlockNextBlockId != BlockId()) {
          biggerBlock.thisBlockNextBlockId = thisBlockNextBlockId
          DirectMemoryManager.updateBlockIds(thisBlockNextBlockId, UpdateBlockPosition.PRE, biggerBlock.thisBlockId)
        }
        newHeadId = smallerBlock.thisBlockId
      }
      // middle block split
      else if (thisBlockPreBlockId != BlockId() && thisBlockNextBlockId != BlockId()) {
        smallerBlock.thisBlockPreBlockId = thisBlockPreBlockId
        smallerBlock.thisBlockNextBlockId = biggerBlock.thisBlockId

        biggerBlock.thisBlockPreBlockId = smallerBlock.thisBlockId
        biggerBlock.thisBlockNextBlockId = thisBlockNextBlockId

        DirectMemoryManager.updateBlockIds(thisBlockPreBlockId, UpdateBlockPosition.NEXT, smallerBlock.thisBlockId)
        DirectMemoryManager.updateBlockIds(thisBlockNextBlockId, UpdateBlockPosition.PRE, biggerBlock.thisBlockId)
      }
      // last block split
      else {
        smallerBlock.thisBlockPreBlockId = thisBlockPreBlockId
        smallerBlock.thisBlockNextBlockId = biggerBlock.thisBlockId
        DirectMemoryManager.updateBlockIds(thisBlockPreBlockId, UpdateBlockPosition.NEXT, smallerBlock.thisBlockId)

        biggerBlock.thisBlockPreBlockId = smallerBlock.thisBlockId
      }

      deleteLog.enqueue(thisBlockId)
      DirectMemoryManager.putInitBlockToDirectBuffer(smallerBlock, smallerArray.toArray, true)
      DirectMemoryManager.putInitBlockToDirectBuffer(biggerBlock, biggerArray.toArray, true)
    }
    newHeadId
  }

  def isExist(endNodeId: Long): Boolean = {
    DirectMemoryManager.getBlockDataArray(thisBlockId).contains(endNodeId)
  }

  def delete(nodeId: Long): Int = {
    var arraySize: Int = 0
    var dataArray = DirectMemoryManager.getBlockDataArray(thisBlockId)
    if (dataArray.contains(nodeId)){
      dataArray -= nodeId
      if (dataArray.nonEmpty){
        dataArray = dataArray.sorted
        arraySize = dataArray.length
        DirectMemoryManager.updateBlockData(thisBlockId, dataArray.toArray)
      }
    }else arraySize = dataArray.length
    arraySize
  }
}

case class BlockInfo(blockId: BlockId, preBlockId: BlockId, nextBlockId: BlockId, minNodeId: Long, maxNodeId: Long, arrayUsedSize: Short){}

// pageId: in which directBuffer
case class BlockId(pageId: Short = 0, offset: Int = 0) {}

class GetAllBlockNodesId(manager: OutGoingEdgeBlockManager) extends Iterator[Long] {
  var block: BlockInfo = _
  var nextBlockId: BlockId = _
  var arrayUsedSize:Short = _
  var count = 0
  var isFinish = false
  var dataArray: ArrayBuffer[Long] = _

  if (manager.getBeginBlockId != BlockId()){
    block = DirectMemoryManager.getBlockInfo(manager.getBeginBlockId)
    nextBlockId = block.nextBlockId
    arrayUsedSize = block.arrayUsedSize
    dataArray = DirectMemoryManager.getBlockDataArray(manager.getBeginBlockId)
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
        block = DirectMemoryManager.getBlockInfo(nextBlockId)
        dataArray = DirectMemoryManager.getBlockDataArray(nextBlockId)
        nextBlockId = block.nextBlockId
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
    if (!isFinish) {
      dataArray(count - 1)
    }
    else throw new NoNextNodeIdException
  }
}

class NoBlockToGetException extends Exception {
  override def getMessage: String = "No such block to get"
}

class NoNextNodeIdException extends Exception{
  override def getMessage: String = "next on empty iterator"

}