package cn.pandadb.kernel.direct

import scala.collection.mutable.ArrayBuffer


class BlockManager(directBuffer: ArrayBuffer[DataBlock], blockSize: Int) {
  def put(relation: Relation): Unit = {
    if (directBuffer.isEmpty) {
      val block = new DataBlock(directBuffer.size, blockSize, relation.id, relation.id, directBuffer)
      block.put(relation)
      directBuffer += block
    }
    else {
      // rules
      val rule1 = directBuffer.find(p => {
        if (p.arrayUsedSize <= blockSize && p.blockMinId <= relation.id && p.blockMaxId >= relation.id) {
          true
        }
        else false
      })

      if (rule1.isDefined) rule1.get.put(relation)
      else searchBlock(directBuffer, relation)
    }
  }

  def delete(id: Long): Unit = {
    val blockIndex = directBuffer.indexWhere(p => {
      p.blockMinId <= id && p.blockMaxId >= id
    })
    if (blockIndex != -1) {
      val block = directBuffer(blockIndex)
      block.delete(id)
      if (block.arrayUsedSize == 0) {
        directBuffer -= block
      }
    }
  }
  def searchBlock(directBuffer: ArrayBuffer[DataBlock], relation: Relation): Unit ={
    // between A.max and B.min
    if (directBuffer.size == 1 && directBuffer(0).arrayUsedSize < blockSize) {
      directBuffer(0).put(relation)
    }
    else{
      var flag = true
      var index = 0
      while (index != (directBuffer.size - 1) && flag){
        val leftBlock = directBuffer(index)
        val rightBlock = directBuffer(index + 1)
        if (leftBlock.blockMaxId < relation.id && rightBlock.blockMinId > relation.id){
          if (leftBlock.arrayUsedSize < blockSize) {
            leftBlock.put(relation)
            flag = false
          }
        }
        index += 1
      }
      // not found
      if (flag){
        createNewBlock(relation)
      }
    }
  }
  def createNewBlock(relation: Relation): Unit ={
    val block = new DataBlock(directBuffer.size, blockSize, relation.id, relation.id, directBuffer)
    directBuffer += block
    block.put(relation)
  }
}

class DataBlock(blockIndex: Int, blockSize: Int, minId: Long, maxId: Long, directBuffer: ArrayBuffer[DataBlock], isSplited: Boolean = false) {
  var dataArray: Array[Relation] = new Array[Relation](blockSize)
  var arrayUsedSize = 0
  var blockMinId = minId
  var blockMaxId = maxId
  var thisBlockIndex = blockIndex
  var isSplit = isSplited

  //todo: check delete log
  def put(relation: Relation): Unit = {
    if (arrayUsedSize < dataArray.size) {
      insertToArray(relation) // sorted
      if (arrayUsedSize == 1) {
        blockMinId = dataArray(0).id
        blockMaxId = dataArray(0).id
      } else {
        if (relation.id <= blockMinId) blockMinId = relation.id
        if (relation.id >= blockMaxId) blockMaxId = relation.id
      }
      sortBlocks(directBuffer)
    }
    else {
      // array full， split it
      val splitIndex = (blockSize + 1) / 2
      val smallerBlock = new DataBlock(thisBlockIndex, blockSize, 0, 0, directBuffer, true)
      val biggerBlock = new DataBlock(directBuffer.size, blockSize, 0, 0, directBuffer, true)

      //set min max
      if (relation.id < dataArray(0).id) {
        smallerBlock.dataArray(0) = relation
        for (i <- 1 until splitIndex) smallerBlock.dataArray(i) = dataArray(i)
        for (i <- splitIndex until blockSize) biggerBlock.dataArray(i - splitIndex) = dataArray(i)

        smallerBlock.blockMaxId = smallerBlock.dataArray(splitIndex - 1).id
        smallerBlock.blockMinId = smallerBlock.dataArray(0).id
        biggerBlock.blockMaxId = biggerBlock.dataArray(splitIndex - 1).id
        biggerBlock.blockMinId = biggerBlock.dataArray(0).id

      }
      else if (relation.id > dataArray(blockSize - 1).id) {
        for (i <- 0 until splitIndex) smallerBlock.dataArray(i) = dataArray(i)
        for (i <- splitIndex until blockSize) biggerBlock.dataArray(i - splitIndex) = dataArray(i)
        biggerBlock.dataArray(blockSize - splitIndex) = relation

        smallerBlock.blockMaxId = smallerBlock.dataArray(splitIndex - 1).id
        smallerBlock.blockMinId = smallerBlock.dataArray(0).id
        biggerBlock.blockMaxId = biggerBlock.dataArray(splitIndex - 1).id
        biggerBlock.blockMinId = biggerBlock.dataArray(0).id
      }
      else {
        //check is bigger or smaller than mid number
        if (relation.id < dataArray(splitIndex - 1).id) {
          for (i <- 0 until splitIndex - 1) smallerBlock.put(dataArray(i))
          smallerBlock.put(relation)
          for (i <- (splitIndex - 1) until blockSize) biggerBlock.put(dataArray(i))
        } else {
          for (i <- 0 until splitIndex) smallerBlock.put(dataArray(i))
          for (i <- (splitIndex) until blockSize) biggerBlock.put(dataArray(i))
          biggerBlock.put(relation)
        }
      }
      smallerBlock.arrayUsedSize = (blockSize + 1) / 2
      biggerBlock.arrayUsedSize = (blockSize + 1) / 2
      directBuffer += biggerBlock
      directBuffer(thisBlockIndex) = smallerBlock
      sortBlocks(directBuffer)
    }
  }

  def delete(id: Long): Unit = {
    arrayUsedSize -= 1
    isSplit = false
    if (id == dataArray(0).id) {
      for (i <- 1 to arrayUsedSize) {
        dataArray(i - 1) = dataArray(i)
      }
      dataArray(arrayUsedSize) = null
    }
    else if (id == dataArray(arrayUsedSize).id) {
      dataArray(arrayUsedSize) = null
    }
    else {
      var index = 0
      while (dataArray(index).id != id) {
        index += 1
      }
      for (i <- index until arrayUsedSize) {
        dataArray(i) = dataArray(i + 1)
      }
      dataArray(arrayUsedSize) = null
    }
  }


  def insertToArray(target: Relation): Unit = {
    dataArray(arrayUsedSize) = target
    arrayUsedSize += 1

    if (arrayUsedSize > 1) {
      for (i <- Range(1, arrayUsedSize, 1)) {
        var j = i - 1
        val tmp = dataArray(i)
        while (j >= 0 && dataArray(j).id > tmp.id) {
          dataArray(j + 1) = dataArray(j)
          j -= 1
        }
        dataArray(j + 1) = tmp
      }
    }
  }
  // todo: pre next
  def sortBlocks(blocks: ArrayBuffer[DataBlock]): Unit = {
    for (i <- Range(1, blocks.size, 1)) {
      var j = i - 1
      val tmp = blocks(i)
      while (j >= 0 && blocks(j).blockMinId > tmp.blockMaxId) {
        blocks(j + 1) = blocks(j)
        j -= 1
      }
      blocks(j + 1) = tmp
    }
    for (i <- blocks.indices) blocks(i).thisBlockIndex = i
  }
}

case class Relation(id: Long, typeId: Int, from: Long, to: Long) {

}