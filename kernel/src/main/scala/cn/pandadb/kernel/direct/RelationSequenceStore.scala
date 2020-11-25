package cn.pandadb.kernel.direct

import cn.pandadb.kernel.store.StoredRelation
import io.netty.buffer.{ByteBuf, Unpooled}

import scala.collection.mutable.ArrayBuffer

class RelationSequenceManager(relationNum: Int = 10000000) {
  val bufOfRelationNum = relationNum
  val DATA_LENGTH = 20 // 8 + 8 + 4
  val MAX_CAPACITY = bufOfRelationNum * DATA_LENGTH
  val MAX_RELATION_ID_OF_A_DIRECTBUFFER = MAX_CAPACITY / DATA_LENGTH

  val directBufferPageArray = new ArrayBuffer[ByteBuf]()

  private def mallocDirectBuffer(): Unit = {
    val directBuffer = Unpooled.directBuffer(MAX_CAPACITY)
    directBuffer.setZero(0, MAX_CAPACITY)
    directBufferPageArray.append(directBuffer)
  }

  def addRelation(rId: Long, from: Long, to: Long, label: Int): Unit = {
    var directBuffer: ByteBuf = null
    var offset: Int = 0

    val index = (rId - 1) / MAX_RELATION_ID_OF_A_DIRECTBUFFER
    if (index + 1 > directBufferPageArray.length) {
      val distance = index + 1 - directBufferPageArray.length
      for (i <- 1 to distance.toInt) {
        mallocDirectBuffer()
      }
      directBuffer = directBufferPageArray.last
      offset = ((rId - 1) - (directBufferPageArray.length - 1) * MAX_RELATION_ID_OF_A_DIRECTBUFFER).toInt * DATA_LENGTH
    }
    else {
      directBuffer = directBufferPageArray(index.toInt)
      offset = ((rId - 1) - index * MAX_RELATION_ID_OF_A_DIRECTBUFFER).toInt * DATA_LENGTH
    }
    directBuffer.setLong(offset, from)
    directBuffer.setLong(offset + 8, to)
    directBuffer.setInt(offset + 16, label)
  }

  def deleteRelation(rId: Long): Unit = {
    val position = getRelationIndexAndOffSet(rId)
    val directBuffer = directBufferPageArray(position._1)
    directBuffer.setLong(position._2, 0)
    directBuffer.setLong(position._2 + 8, 0)
    directBuffer.setInt(position._2 + 16, 0)
  }

  def getRelation(rId: Long): StoredRelation = {
    val position = getRelationIndexAndOffSet(rId)
    val directBuffer = directBufferPageArray(position._1)
    val from = directBuffer.getLong(position._2)
    val to = directBuffer.getLong(position._2 + 8)
    val labelId = directBuffer.getInt(position._2 + 16)
    if (from == 0 && to == 0 && labelId == 0) throw new NoRelationGetException
    StoredRelation(rId, from, to, labelId)
  }

  private def getRelationIndexAndOffSet(rId: Long): (Int, Int) = {
    if (rId <= 0) throw new IllegalArgumentException("not a illegal relation id")
    val index = (rId - 1) / MAX_RELATION_ID_OF_A_DIRECTBUFFER
    if (index + 1 > directBufferPageArray.length) {
      throw new NoRelationGetException
    }
    val offset = ((rId - 1) - index * MAX_RELATION_ID_OF_A_DIRECTBUFFER).toInt * DATA_LENGTH
    (index.toInt, offset)
  }

  def getAllRelations(): Iterator[StoredRelation] = {
    new RelationIterator(this)
  }

  def clear(): Unit = {
    directBufferPageArray.foreach(buf => buf.release())
    directBufferPageArray.clear()
  }
}

class NoRelationGetException extends Exception {
  override def getMessage: String = "no relation get"
}

class RelationIterator(manager: RelationSequenceManager) extends Iterator[StoredRelation] {
  var dataArray = manager.directBufferPageArray
  var bufferOfRelationNum = manager.bufOfRelationNum
  var relationBlockSize = manager.DATA_LENGTH
  var directBuffer: ByteBuf = null

  var arrayLength = 0
  var indexCount = 0

  var rId: Long = 0
  var queryResult: StoredRelation = null

  if (dataArray.nonEmpty) {
    directBuffer = dataArray(0)
  }
  else {
    throw new NoNextRelationIdException
  }

  override def hasNext: Boolean = {
    var isStop = false
    var isFound = false
    while (!isStop) {
      if (arrayLength < dataArray.length) {
        if (indexCount < bufferOfRelationNum) {
          val offset = indexCount * relationBlockSize
          indexCount += 1
          val from = directBuffer.getLong(offset)
          rId += 1
          if (from != 0) {
            isFound = true
            isStop = true
            val to = directBuffer.getLong(offset + 8)
            val labelId = directBuffer.getInt(offset + 16)
            queryResult = StoredRelation(rId, from, to, labelId)
          }
        }
        else {
          arrayLength += 1
          indexCount = 0
          if (arrayLength < dataArray.length) directBuffer = dataArray(arrayLength)
        }
      }
      else{
        isStop = true
        isFound = false
      }
    }
    isFound
  }

  override def next(): StoredRelation = {
    queryResult
  }
}

class NoNextRelationIdException extends Exception{
  override def getMessage: String = "next on empty iterator"
}