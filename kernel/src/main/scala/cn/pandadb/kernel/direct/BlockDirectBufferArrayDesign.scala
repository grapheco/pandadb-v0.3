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
        if (p.arrayUsedSize == blockSize && p.blockMinId <= relation.id && p.blockMaxId >= relation.id) {
          true
        }
        else false
      })
      val rule2 = directBuffer.find(p=>{
        p.isSplit && p.blockMinId < relation.id && p.blockMaxId > relation.id
      })
      val rule3 = directBuffer.filter(p=>{
        p.arrayUsedSize < blockSize
      })
      // first search marked by split's block
      if (rule2.isDefined) rule2.get.put(relation)
      //second search full block
      else if (rule1.isDefined) rule1.get.put(relation)
      //third search not full block and calculate relation.id's distance for each block, choose the min distance block
      else if (rule3.size > 1) {
        var minDistance: Int = Int.MaxValue
        var index = 0
        for (b <- rule3){
          val distance = math.abs(b.blockMaxId - relation.id)
          if (distance < minDistance){
            minDistance = distance.toInt
            index = b.thisBlockIndex
          }
        }
        directBuffer(index).put(relation)
      }
      // only one block
      else if (directBuffer.size == 1) directBuffer(0).put(relation)
      else {
        val block = new DataBlock(directBuffer.size, blockSize, relation.id, relation.id, directBuffer)
        block.put(relation)
        directBuffer += block
      }
    }
  }
  def delete(id: Long): Unit ={
    val blockIndex = directBuffer.indexWhere(p=>{
      p.blockMinId<= id && p.blockMaxId >= id
    })
    if (blockIndex != -1){
      val block = directBuffer(blockIndex)
      block.delete(id)
      if (block.arrayUsedSize == 0){
        directBuffer -= block
      }
    }
  }
}

class DataBlock(blockIndex: Int, blockSize: Int, minId: Long, maxId: Long, directBuffer: ArrayBuffer[DataBlock], isSplited: Boolean=false) {
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
      if (arrayUsedSize == 1){
        blockMinId = dataArray(0).id
        blockMaxId = dataArray(0).id
      }else{
        if (relation.id <= blockMinId) blockMinId = relation.id
        if (relation.id >= blockMaxId) blockMaxId = relation.id
      }
    }
    else {
      // array full， split it
      val splitIndex = (blockSize + 1) / 2
      val smallerBlock = new DataBlock(thisBlockIndex, blockSize, 0, 0, directBuffer, true)
      val biggerBlock = new DataBlock(directBuffer.size, blockSize, 0, 0, directBuffer, true)

      //set min max
      if (relation.id < dataArray(0).id){
        smallerBlock.dataArray(0) = relation
        for (i <- 1 until splitIndex) smallerBlock.dataArray(i) = dataArray(i)
        for (i <- splitIndex until blockSize) biggerBlock.dataArray(i-splitIndex) = dataArray(i)

        smallerBlock.blockMaxId = smallerBlock.dataArray(splitIndex - 1).id
        smallerBlock.blockMinId = smallerBlock.dataArray(0).id
        biggerBlock.blockMaxId = smallerBlock.dataArray(splitIndex - 1).id
        biggerBlock.blockMinId = smallerBlock.dataArray(0).id

      }
      else if(relation.id > dataArray(blockSize-1).id){
        for (i <- 0 until splitIndex) smallerBlock.dataArray(i) = dataArray(i)
        for (i <- splitIndex until blockSize) biggerBlock.dataArray(i-splitIndex) = dataArray(i)
        biggerBlock.dataArray(blockSize-splitIndex) = relation

        smallerBlock.blockMaxId = smallerBlock.dataArray(splitIndex - 1).id
        smallerBlock.blockMinId = smallerBlock.dataArray(0).id
        biggerBlock.blockMaxId = smallerBlock.dataArray(splitIndex - 1).id
        biggerBlock.blockMinId = smallerBlock.dataArray(0).id
      }
      else{
        //check is bigger or smaller than mid number
        if (relation.id < dataArray(splitIndex-1).id){
          for (i <- 0 until splitIndex - 1) smallerBlock.put(dataArray(i))
          smallerBlock.put(relation)
          for (i <- (splitIndex - 1) until blockSize) biggerBlock.put(dataArray(i))
        }else{
          for (i <- 0 until splitIndex) smallerBlock.put(dataArray(i))
          for (i <- (splitIndex) until blockSize) biggerBlock.put(dataArray(i))
          biggerBlock.put(relation)
        }
      }
      directBuffer += biggerBlock
      directBuffer(thisBlockIndex) = smallerBlock
    }
  }

  def delete(id: Long): Unit ={
    arrayUsedSize -= 1
    isSplit = false
    if (id == dataArray(0).id){
      for (i <- 1 to arrayUsedSize){
        dataArray(i - 1) = dataArray(i)
      }
      dataArray(arrayUsedSize) = null
      blockMinId = dataArray(0).id
    }
    else if(id == dataArray(arrayUsedSize).id){
      dataArray(arrayUsedSize) = null
      blockMaxId = dataArray(arrayUsedSize - 1).id
    }
    else {
      var index = 0
      while (dataArray(index).id != id){
        index += 1
      }
      for (i <- index until arrayUsedSize){
        dataArray(i) = dataArray(i + 1)
      }
      dataArray(arrayUsedSize) = null
    }
  }


  def insertToArray(target: Relation): Unit = {
    dataArray(arrayUsedSize) = target
    arrayUsedSize += 1

    if (arrayUsedSize > 1){
      for (i <- Range(1, arrayUsedSize, 1)){
        var j = i - 1
        val tmp = dataArray(i)
        while (j >= 0 && dataArray(j).id > tmp.id){
          dataArray(j + 1) = dataArray(j)
          j -= 1
        }
        dataArray(j + 1) = tmp
      }
    }
  }
}

case class Relation(id: Long, from: Long, to: Long, labelId: Int)