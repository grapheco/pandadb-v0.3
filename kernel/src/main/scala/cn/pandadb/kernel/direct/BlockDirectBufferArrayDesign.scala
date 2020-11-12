package cn.pandadb.kernel.direct

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DirectMemoryManager{
  val deleteLog: mutable.Queue[Int] = mutable.Queue[Int]()
  val directBuffer = new ArrayBuffer[EndNodesBlock]()
  val BLOCK_SIZE = 3

  def generateBlock(): EndNodesBlock = {
    new EndNodesBlock(-1, -1, -1, -1, -1, BLOCK_SIZE, deleteLog)
  }

  def generateBlockId(): (Int, Boolean) = {
    var isFromDelete = false
    var blockId = directBuffer.size
    if (deleteLog.nonEmpty) {
      blockId = deleteLog.dequeue()
      isFromDelete = true
    }
    (blockId, isFromDelete)
  }

  def putBlockToDirectBuffer(newBlock: EndNodesBlock, index: Int, isReplace: Boolean): Unit = {
    if (isReplace) {
      directBuffer(index) = newBlock
    } else {
      directBuffer += newBlock
    }
  }
  def getBlock(blockId: Int): EndNodesBlock ={
    directBuffer(blockId)
  }
}

class OutGoingEdgeBlockManager(startBlockId: Int = -1) {

  private var beginBlockId = startBlockId

  def getBeginBlockId: Int = {
    beginBlockId
  }

  def put(nodeId: Long): Int = {
    // no block
    if (beginBlockId == -1) {
      val getBlockId = DirectMemoryManager.generateBlockId()

      beginBlockId = getBlockId._1
      val newBlock = DirectMemoryManager.generateBlock()
      newBlock.put(nodeId)
      newBlock.thisBlockId = beginBlockId
      newBlock.thisBlockMinNodeId = nodeId
      newBlock.thisBlockMaxNodeId = nodeId

      DirectMemoryManager.putBlockToDirectBuffer(newBlock, beginBlockId, getBlockId._2)
    }
    // have block
    else {
      var flag = true
      var queryBlock = DirectMemoryManager.getBlock(beginBlockId)
      while (flag) {
        val isFound = queryBlockToInsertStrategy(queryBlock, nodeId)
        if (!isFound) {
          if (queryBlock.thisBlockNextBlockId != -1) {
            queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
          } else {
            flag = false
            throw new Exception("unknown situation error....please report")
          }
        }
        else flag = false
      }
    }
    beginBlockId
  }

  def queryBlockToInsertStrategy(queryBlock: EndNodesBlock, nodeId: Long): Boolean = {
    var isFound = false
    var preBlock: EndNodesBlock = null
    var nextBlock: EndNodesBlock = null
    if (queryBlock.thisBlockPreBlockId != -1) preBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockPreBlockId)
    if (queryBlock.thisBlockNextBlockId != -1) nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)

    if (nodeId > queryBlock.thisBlockMinNodeId && nodeId < queryBlock.thisBlockMaxNodeId) {
      queryBlock.put(nodeId)
      isFound = true
    }
    else if (queryBlock.arrayUsedSize < DirectMemoryManager.BLOCK_SIZE) {
      // put to this block depends on preBlock max and nextBlock min
      if (preBlock == null && nextBlock == null) {
        queryBlock.put(nodeId)
        isFound = true
      }
      else if (preBlock == null && nextBlock != null) {
        if (nodeId < nextBlock.thisBlockMinNodeId) {
          queryBlock.put(nodeId)
          isFound = true
        }
      }
      else if (preBlock != null && nextBlock == null) {
        if (nodeId > preBlock.thisBlockMaxNodeId) {
          queryBlock.put(nodeId)
          isFound = true
        }
      }
      else {
        if (nodeId > preBlock.thisBlockMaxNodeId && nodeId < nextBlock.thisBlockMinNodeId) {
          queryBlock.put(nodeId)
          isFound = true
        }
      }
    }
    else if (queryBlock.arrayUsedSize == DirectMemoryManager.BLOCK_SIZE) {
      // new head
      if (preBlock == null && nodeId < queryBlock.thisBlockMinNodeId) {
        val index = DirectMemoryManager.generateBlockId()
        val newBlock = DirectMemoryManager.generateBlock()
        newBlock.put(nodeId)

        newBlock.thisBlockNextBlockId = queryBlock.thisBlockId
        newBlock.thisBlockId = index._1
        newBlock.thisBlockMinNodeId = nodeId
        newBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockToDirectBuffer(newBlock, index._1, index._2)

        queryBlock.thisBlockPreBlockId = newBlock.thisBlockId
        beginBlockId = newBlock.thisBlockId

        isFound = true
      }
      // insert to left
      else if (preBlock != null && nodeId > preBlock.thisBlockMaxNodeId && nodeId < queryBlock.thisBlockMinNodeId) {
        val index = DirectMemoryManager.generateBlockId()
        //        val leftBlock = new EndNodesBlock(preBlock.thisBlockId, queryBlock.thisBlockId, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
        val leftBlock = DirectMemoryManager.generateBlock()
        leftBlock.put(nodeId)

        leftBlock.thisBlockPreBlockId = preBlock.thisBlockId
        leftBlock.thisBlockNextBlockId = queryBlock.thisBlockId
        leftBlock.thisBlockId = index._1
        leftBlock.thisBlockMinNodeId = nodeId
        leftBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockToDirectBuffer(leftBlock, index._1, index._2)

        preBlock.thisBlockNextBlockId = leftBlock.thisBlockId
        queryBlock.thisBlockPreBlockId = leftBlock.thisBlockId

        isFound = true
      }
      // insert to right
      else if (nextBlock != null && nodeId < nextBlock.thisBlockMinNodeId && nodeId > queryBlock.thisBlockMaxNodeId) {
        val index = DirectMemoryManager.generateBlockId()
        //        val rightBlock = new EndNodesBlock(queryBlock.thisBlockId, nextBlock.thisBlockId, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
        val rightBlock = DirectMemoryManager.generateBlock()
        rightBlock.put(nodeId)

        rightBlock.thisBlockPreBlockId = queryBlock.thisBlockId
        rightBlock.thisBlockNextBlockId = nextBlock.thisBlockId
        rightBlock.thisBlockId = index._1
        rightBlock.thisBlockMinNodeId = nodeId
        rightBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockToDirectBuffer(rightBlock, index._1, index._2)

        queryBlock.thisBlockNextBlockId = rightBlock.thisBlockId
        nextBlock.thisBlockPreBlockId = rightBlock.thisBlockId
        isFound = true
      }
      // new tail
      else if (nextBlock == null && nodeId > queryBlock.thisBlockMaxNodeId) {
        val index = DirectMemoryManager.generateBlockId()
        //        val tailBlock = new EndNodesBlock(queryBlock.thisBlockId, -1, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
        val tailBlock = DirectMemoryManager.generateBlock()
        tailBlock.put(nodeId)

        tailBlock.thisBlockPreBlockId = queryBlock.thisBlockId
        tailBlock.thisBlockId = index._1
        tailBlock.thisBlockMinNodeId = nodeId
        tailBlock.thisBlockMaxNodeId = nodeId

        DirectMemoryManager.putBlockToDirectBuffer(tailBlock, index._1, index._2)

        queryBlock.thisBlockNextBlockId = tailBlock.thisBlockId
        isFound = true
      }
    }
    isFound
  }

  def delete(nodeId: Long): Unit = {
    var queryBlock = DirectMemoryManager.getBlock(beginBlockId)
    var flag = true
    while (flag) {
      if (nodeId >= queryBlock.thisBlockMinNodeId && nodeId <= queryBlock.thisBlockMaxNodeId) {
        val remainSize = queryBlock.delete(nodeId)
        if (remainSize == 0) {
          // delete only 1 block
          if (queryBlock.thisBlockPreBlockId == -1 && queryBlock.thisBlockNextBlockId == -1) {
            beginBlockId = -1
            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
          // delete head block
          else if (queryBlock.thisBlockPreBlockId == -1) {
            val nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
            beginBlockId = nextBlock.thisBlockId
            nextBlock.thisBlockPreBlockId = -1

            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
          // delete middle block
          else if (queryBlock.thisBlockPreBlockId != -1 && queryBlock.thisBlockNextBlockId != -1) {
            val preBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockPreBlockId)
            val nextBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
            preBlock.thisBlockNextBlockId = nextBlock.thisBlockId
            nextBlock.thisBlockPreBlockId = preBlock.thisBlockId

            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
          // delete last block
          else {
            val preBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockPreBlockId)
            preBlock.thisBlockNextBlockId = -1

            DirectMemoryManager.deleteLog.enqueue(queryBlock.thisBlockId)
          }
        }
        flag = false
      }
      else {
        if (queryBlock.thisBlockNextBlockId != -1) {
          queryBlock = DirectMemoryManager.getBlock(queryBlock.thisBlockNextBlockId)
        } else {
          flag = false
        }
      }
    }
  }

  def queryByLink(startBlockId: Int): Unit = {
    val startIndex = startBlockId
    var flag = true
    if (startIndex != -1) {
      var block = DirectMemoryManager.getBlock(startIndex)
      while (flag) {
        block.nodeIdArray.foreach(println)
        if (block.thisBlockNextBlockId != -1) {
          block = DirectMemoryManager.getBlock(block.thisBlockNextBlockId)
        }
        else flag = false
        println("===========================================")
      }
    }
  }
}

// blockSize should be odd number
class EndNodesBlock(preBlock: Int, nextBlock: Int, blockId: Int, maxNodeId: Long, minNodeId: Long, blockSize: Int,
                    deleteLog: mutable.Queue[Int]) {
  val nodeIdArray: Array[Long] = new Array[Long](blockSize)
  var arrayUsedSize = 0

  var thisBlockMinNodeId: Long = minNodeId
  var thisBlockMaxNodeId: Long = maxNodeId
  var thisBlockId: Int = blockId
  var thisBlockNextBlockId: Int = nextBlock
  var thisBlockPreBlockId: Int = preBlock

  def put(nodeId: Long): Unit = {
    if (arrayUsedSize < nodeIdArray.length) {
      insertToArray(nodeId)
      if (arrayUsedSize != 1) {
        if (nodeId < thisBlockMinNodeId) thisBlockMinNodeId = nodeId
        if (nodeId > thisBlockMaxNodeId) thisBlockMaxNodeId = nodeId
      }
    }
    // split block
    else {
      val splitAt = blockSize / 2
      val smallerBlock = DirectMemoryManager.generateBlock()
      val biggerBlock = DirectMemoryManager.generateBlock()

      if (nodeId < nodeIdArray(splitAt)) {
        // nodeId belong to smaller block
        for (i <- 0 until splitAt) smallerBlock.put(nodeIdArray(i))
        smallerBlock.put(nodeId)
        for (i <- splitAt until blockSize) biggerBlock.put(nodeIdArray(i))
      }
      // nodeId belong to bigger block
      else {
        for (i <- 0 to splitAt) smallerBlock.put(nodeIdArray(i))
        for (i <- (splitAt + 1) until blockSize) biggerBlock.put(nodeIdArray(i))
        biggerBlock.put(nodeId)
      }
      smallerBlock.thisBlockMinNodeId = smallerBlock.nodeIdArray(0)
      smallerBlock.thisBlockMaxNodeId = smallerBlock.nodeIdArray(splitAt)
      biggerBlock.thisBlockMinNodeId = biggerBlock.nodeIdArray(0)
      biggerBlock.thisBlockMaxNodeId = biggerBlock.nodeIdArray(splitAt)

      val biggerBlockId = DirectMemoryManager.generateBlockId()

      // first block split
      if (thisBlockPreBlockId == -1) {

        smallerBlock.thisBlockNextBlockId = biggerBlockId._1
        smallerBlock.thisBlockId = thisBlockId

        biggerBlock.thisBlockPreBlockId = thisBlockId
        biggerBlock.thisBlockId = biggerBlockId._1
        if (thisBlockNextBlockId != -1) {
          biggerBlock.thisBlockNextBlockId = thisBlockNextBlockId
        }
      }
      // middle block split
      else if (thisBlockPreBlockId != -1 && thisBlockNextBlockId != -1) {
        smallerBlock.thisBlockPreBlockId = thisBlockPreBlockId
        smallerBlock.thisBlockId = thisBlockId
        smallerBlock.thisBlockNextBlockId = biggerBlockId._1

        biggerBlock.thisBlockPreBlockId = thisBlockId
        biggerBlock.thisBlockId = biggerBlockId._1
        biggerBlock.thisBlockNextBlockId = thisBlockNextBlockId
      }
      // last block split
      else {
        smallerBlock.thisBlockPreBlockId = thisBlockPreBlockId
        smallerBlock.thisBlockId = thisBlockId
        smallerBlock.thisBlockNextBlockId = biggerBlockId._1

        biggerBlock.thisBlockPreBlockId = thisBlockId
        biggerBlock.thisBlockId = biggerBlockId._1
      }

      DirectMemoryManager.putBlockToDirectBuffer(smallerBlock, smallerBlock.thisBlockId, isReplace = true)
      DirectMemoryManager.putBlockToDirectBuffer(biggerBlock, biggerBlockId._1, biggerBlockId._2)
    }
  }

  def delete(nodeId: Long): Int = {
    for (i <- 0 until arrayUsedSize) {
      if (nodeIdArray(i) == nodeId) {
        arrayUsedSize -= 1
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
    arrayUsedSize += 1

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
