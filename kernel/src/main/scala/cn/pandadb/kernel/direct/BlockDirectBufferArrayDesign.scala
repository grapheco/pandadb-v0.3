package cn.pandadb.kernel.direct

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GlobalManager() {
  val deleteLog = mutable.Queue[Int]()
  val directBuffer = new ArrayBuffer[EndNodesBlock]()
  val BLOCK_SIZE = 3

  def generateBlock(): EndNodesBlock = {
    new EndNodesBlock(-1, -1, -1, -1, -1, BLOCK_SIZE, deleteLog, GlobalManager.this)
  }

  def generateBlockIndex(): (Int, Boolean) = {
    var isFromDelete = false
    var blockIndex = directBuffer.size
    if (deleteLog.nonEmpty) {
      blockIndex = deleteLog.dequeue()
      isFromDelete = true
    }
    (blockIndex, isFromDelete)
  }

  def putBlockToDirectBuffer(newBlock: EndNodesBlock, index: Int, isReplace: Boolean): Unit = {
    if (isReplace) {
      directBuffer(index) = newBlock
    } else {
      directBuffer += newBlock
    }
  }
  def getBlock(blockIndex: Int): EndNodesBlock ={
    directBuffer(blockIndex)
  }
}

class FromNodeBlockManager(globalManager: GlobalManager) {
  private var beginBlockIndex = -1

  def getBeginBlockIndex(): Int = {
    beginBlockIndex
  }

  def put(nodeId: Long): Unit = {
    // no block
    if (beginBlockIndex == -1) {
      val genBlockIndex = globalManager.generateBlockIndex()

      beginBlockIndex = genBlockIndex._1
      val newBlock = globalManager.generateBlock()
      newBlock.put(nodeId)
      newBlock.thisBlockIndex = beginBlockIndex
      newBlock.thisBlockMinNodeId = nodeId
      newBlock.thisBlockMaxNodeId = nodeId

      globalManager.putBlockToDirectBuffer(newBlock, beginBlockIndex, genBlockIndex._2)
    }
    // have block
    else {
      var flag = true
      var queryBlock = globalManager.getBlock(beginBlockIndex)
      while (flag) {
        val isFound = queryBlockToInsertStrategy(queryBlock, nodeId)
        if (!isFound) {
          if (queryBlock.thisBlockNextBlockIndex != -1) {
            queryBlock = globalManager.getBlock(queryBlock.thisBlockNextBlockIndex)
          } else {
            flag = false
            throw new Exception("unknown situation error....please report")
          }
        }
        else flag = false
      }
    }
  }

  def queryBlockToInsertStrategy(queryBlock: EndNodesBlock, nodeId: Long): Boolean = {
    var isFound = false
    var preBlock: EndNodesBlock = null
    var nextBlock: EndNodesBlock = null
    if (queryBlock.thisBlockPreBlockIndex != -1) preBlock = globalManager.getBlock(queryBlock.thisBlockPreBlockIndex)
    if (queryBlock.thisBlockNextBlockIndex != -1) nextBlock = globalManager.getBlock(queryBlock.thisBlockNextBlockIndex)

    if (nodeId > queryBlock.thisBlockMinNodeId && nodeId < queryBlock.thisBlockMaxNodeId) {
      queryBlock.put(nodeId)
      isFound = true
    }
    else if (queryBlock.arrayUsedSize < globalManager.BLOCK_SIZE) {
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
    else if (queryBlock.arrayUsedSize == globalManager.BLOCK_SIZE) {
      // new head
      if (preBlock == null && nodeId < queryBlock.thisBlockMinNodeId) {
        val index = globalManager.generateBlockIndex()
        val newBlock = globalManager.generateBlock()
        newBlock.put(nodeId)

        newBlock.thisBlockNextBlockIndex = queryBlock.thisBlockIndex
        newBlock.thisBlockIndex = index._1
        newBlock.thisBlockMinNodeId = nodeId
        newBlock.thisBlockMaxNodeId = nodeId

        globalManager.putBlockToDirectBuffer(newBlock, index._1, index._2)

        queryBlock.thisBlockPreBlockIndex = newBlock.thisBlockIndex
        beginBlockIndex = newBlock.thisBlockIndex

        isFound = true
      }
      // insert to left
      else if (preBlock != null && nodeId > preBlock.thisBlockMaxNodeId && nodeId < queryBlock.thisBlockMinNodeId) {
        val index = globalManager.generateBlockIndex()
        //        val leftBlock = new EndNodesBlock(preBlock.thisBlockIndex, queryBlock.thisBlockIndex, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
        val leftBlock = globalManager.generateBlock()
        leftBlock.put(nodeId)

        leftBlock.thisBlockPreBlockIndex = preBlock.thisBlockIndex
        leftBlock.thisBlockNextBlockIndex = queryBlock.thisBlockIndex
        leftBlock.thisBlockIndex = index._1
        leftBlock.thisBlockMinNodeId = nodeId
        leftBlock.thisBlockMaxNodeId = nodeId

        globalManager.putBlockToDirectBuffer(leftBlock, index._1, index._2)

        preBlock.thisBlockNextBlockIndex = leftBlock.thisBlockIndex
        queryBlock.thisBlockPreBlockIndex = leftBlock.thisBlockIndex

        isFound = true
      }
      // insert to right
      else if (nextBlock != null && nodeId < nextBlock.thisBlockMinNodeId && nodeId > queryBlock.thisBlockMaxNodeId) {
        val index = globalManager.generateBlockIndex()
        //        val rightBlock = new EndNodesBlock(queryBlock.thisBlockIndex, nextBlock.thisBlockIndex, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
        val rightBlock = globalManager.generateBlock()
        rightBlock.put(nodeId)

        rightBlock.thisBlockPreBlockIndex = queryBlock.thisBlockIndex
        rightBlock.thisBlockNextBlockIndex = nextBlock.thisBlockIndex
        rightBlock.thisBlockIndex = index._1
        rightBlock.thisBlockMinNodeId = nodeId
        rightBlock.thisBlockMaxNodeId = nodeId

        globalManager.putBlockToDirectBuffer(rightBlock, index._1, index._2)

        queryBlock.thisBlockNextBlockIndex = rightBlock.thisBlockIndex
        nextBlock.thisBlockPreBlockIndex = rightBlock.thisBlockIndex
        isFound = true
      }
      // new tail
      else if (nextBlock == null && nodeId > queryBlock.thisBlockMaxNodeId) {
        val index = globalManager.generateBlockIndex()
        //        val tailBlock = new EndNodesBlock(queryBlock.thisBlockIndex, -1, index._1, nodeId, nodeId, blockSize, directBuffer, deleteLog)
        val tailBlock = globalManager.generateBlock()
        tailBlock.put(nodeId)

        tailBlock.thisBlockPreBlockIndex = queryBlock.thisBlockIndex
        tailBlock.thisBlockIndex = index._1
        tailBlock.thisBlockMinNodeId = nodeId
        tailBlock.thisBlockMaxNodeId = nodeId

        globalManager.putBlockToDirectBuffer(tailBlock, index._1, index._2)

        queryBlock.thisBlockNextBlockIndex = tailBlock.thisBlockIndex
        isFound = true
      }
    }
    isFound
  }

  def delete(nodeId: Long): Unit = {
    var queryBlock = globalManager.getBlock(beginBlockIndex)
    var flag = true
    while (flag) {
      if (nodeId >= queryBlock.thisBlockMinNodeId && nodeId <= queryBlock.thisBlockMaxNodeId) {
        val remainSize = queryBlock.delete(nodeId)
        if (remainSize == 0) {
          // delete only 1 block
          if (queryBlock.thisBlockPreBlockIndex == -1 && queryBlock.thisBlockNextBlockIndex == -1) {
            beginBlockIndex = -1
            globalManager.deleteLog.enqueue(queryBlock.thisBlockIndex)
          }
          // delete head block
          else if (queryBlock.thisBlockPreBlockIndex == -1) {
            val nextBlock = globalManager.getBlock(queryBlock.thisBlockNextBlockIndex)
            beginBlockIndex = nextBlock.thisBlockIndex
            nextBlock.thisBlockPreBlockIndex = -1

            globalManager.deleteLog.enqueue(queryBlock.thisBlockIndex)
          }
          // delete middle block
          else if (queryBlock.thisBlockPreBlockIndex != -1 && queryBlock.thisBlockNextBlockIndex != -1) {
            val preBlock = globalManager.getBlock(queryBlock.thisBlockPreBlockIndex)
            val nextBlock = globalManager.getBlock(queryBlock.thisBlockNextBlockIndex)
            preBlock.thisBlockNextBlockIndex = nextBlock.thisBlockIndex
            nextBlock.thisBlockPreBlockIndex = preBlock.thisBlockIndex

            globalManager.deleteLog.enqueue(queryBlock.thisBlockIndex)
          }
          // delete last block
          else {
            val preBlock = globalManager.getBlock(queryBlock.thisBlockPreBlockIndex)
            preBlock.thisBlockNextBlockIndex = -1

            globalManager.deleteLog.enqueue(queryBlock.thisBlockIndex)
          }
        }
        flag = false
      }
      else {
        if (queryBlock.thisBlockNextBlockIndex != -1) {
          queryBlock = globalManager.getBlock(queryBlock.thisBlockNextBlockIndex)
        } else {
          flag = false
        }
      }
    }
  }

  def queryByLink(): Unit = {
    val startIndex = beginBlockIndex
    var flag = true
    if (startIndex != -1) {
      var block = globalManager.getBlock(startIndex)
      while (flag) {
        block.nodeIdArray.foreach(println)
        if (block.thisBlockNextBlockIndex != -1) {
          block = globalManager.getBlock(block.thisBlockNextBlockIndex)
        }
        else flag = false
        println("===========================================")
      }
    }
  }
}

// blockSize should be odd number
class EndNodesBlock(preBlock: Int, nextBlock: Int, blockIndex: Int, maxNodeId: Long, minNodeId: Long, blockSize: Int,
                    deleteLog: mutable.Queue[Int], globalManager: GlobalManager) {
  val nodeIdArray: Array[Long] = new Array[Long](blockSize)
  var arrayUsedSize = 0

  var thisBlockMinNodeId = minNodeId
  var thisBlockMaxNodeId = maxNodeId
  var thisBlockIndex = blockIndex
  var thisBlockNextBlockIndex = nextBlock
  var thisBlockPreBlockIndex = preBlock

  def put(nodeId: Long): Unit = {
    if (arrayUsedSize < nodeIdArray.size) {
      insertToArray(nodeId)
      if (arrayUsedSize != 1) {
        if (nodeId < thisBlockMinNodeId) thisBlockMinNodeId = nodeId
        if (nodeId > thisBlockMaxNodeId) thisBlockMaxNodeId = nodeId
      }
    }
    // split block
    else {
      val splitAt = blockSize / 2
      val smallerBlock = globalManager.generateBlock()
      val biggerBlock = globalManager.generateBlock()

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

      val biggerBlockIndex = globalManager.generateBlockIndex()

      // first block split
      if (thisBlockPreBlockIndex == -1) {

        smallerBlock.thisBlockNextBlockIndex = biggerBlockIndex._1
        smallerBlock.thisBlockIndex = thisBlockIndex

        biggerBlock.thisBlockPreBlockIndex = thisBlockIndex
        biggerBlock.thisBlockIndex = biggerBlockIndex._1
        if (thisBlockNextBlockIndex != -1) {
          biggerBlock.thisBlockNextBlockIndex = thisBlockNextBlockIndex
        }
      }
      // middle block split
      else if (thisBlockPreBlockIndex != -1 && thisBlockNextBlockIndex != -1) {
        smallerBlock.thisBlockPreBlockIndex = thisBlockPreBlockIndex
        smallerBlock.thisBlockIndex = thisBlockIndex
        smallerBlock.thisBlockNextBlockIndex = biggerBlockIndex._1

        biggerBlock.thisBlockPreBlockIndex = thisBlockIndex
        biggerBlock.thisBlockIndex = biggerBlockIndex._1
        biggerBlock.thisBlockNextBlockIndex = thisBlockNextBlockIndex
      }
      // last block split
      else {
        smallerBlock.thisBlockPreBlockIndex = thisBlockPreBlockIndex
        smallerBlock.thisBlockIndex = thisBlockIndex
        smallerBlock.thisBlockNextBlockIndex = biggerBlockIndex._1

        biggerBlock.thisBlockPreBlockIndex = thisBlockIndex
        biggerBlock.thisBlockIndex = biggerBlockIndex._1
      }

      globalManager.putBlockToDirectBuffer(smallerBlock, smallerBlock.thisBlockIndex, true)
      globalManager.putBlockToDirectBuffer(biggerBlock, biggerBlockIndex._1, biggerBlockIndex._2)
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
