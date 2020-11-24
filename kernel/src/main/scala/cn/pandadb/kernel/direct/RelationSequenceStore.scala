package cn.pandadb.kernel.direct

import io.netty.buffer.{ByteBuf, Unpooled}
import scala.collection.mutable.ArrayBuffer

class RelationSequenceManager(blockRelationNum: Int = 10000000) {
  val DATA_LENGTH = 20 // 8 + 8 + 4
  val MAX_CAPACITY = blockRelationNum * DATA_LENGTH
  val MAX_RELATION_ID_OF_A_DIRECTBUFFER = MAX_CAPACITY / DATA_LENGTH

  val directBufferPageArray = new ArrayBuffer[ByteBuf]()

  private def mallocDirectBuffer(): Unit = {
    val directBuffer = Unpooled.directBuffer(MAX_CAPACITY)
    directBufferPageArray.append(directBuffer)
  }

  def addRelation(rId: Long, data: MyRelation): Unit = {
    var directBuffer: ByteBuf = null
    var offset: Int = 0

    val index = (rId-1) / MAX_RELATION_ID_OF_A_DIRECTBUFFER
    if (index + 1 > directBufferPageArray.length) {
      val distance = index + 1 - directBufferPageArray.length
      for (i <- 1 to distance.toInt) {
        mallocDirectBuffer()
      }
      directBuffer = directBufferPageArray.last
      offset = ((rId-1) - (directBufferPageArray.length - 1) * MAX_RELATION_ID_OF_A_DIRECTBUFFER).toInt * DATA_LENGTH
    }
    else {
      directBuffer = directBufferPageArray(index.toInt)
      offset = ((rId-1) - index * MAX_RELATION_ID_OF_A_DIRECTBUFFER).toInt * DATA_LENGTH
    }
    directBuffer.setLong(offset, data.from)
    directBuffer.setLong(offset + 8, data.to)
    directBuffer.setInt(offset + 16, data.label)
  }

  def deleteRelation(rId: Long): Unit ={
    val position = getRelationIndexAndOffSet(rId)
    val directBuffer = directBufferPageArray(position._1)
    directBuffer.setLong(position._2, 0)
    directBuffer.setLong(position._2 + 8, 0)
    directBuffer.setInt(position._2 + 16, 0)
  }

  def getRelation(rId: Long): MyRelation = {
    val position = getRelationIndexAndOffSet(rId)
    val directBuffer = directBufferPageArray(position._1)
    val from = directBuffer.getLong(position._2)
    val to = directBuffer.getLong(position._2 + 8)
    val labelId = directBuffer.getInt(position._2 + 16)
    if (from == 0 && to == 0 && labelId == 0) throw new NoRelationGetException
    MyRelation(from, to, labelId)
  }
  private def getRelationIndexAndOffSet(rId: Long): (Int, Int) ={
    if (rId <= 0) throw  new IllegalArgumentException("not a illegal relation id")
    val index = (rId-1) / MAX_RELATION_ID_OF_A_DIRECTBUFFER
    if (index + 1 > directBufferPageArray.length){
      throw new NoRelationGetException
    }
    val offset = ((rId-1) - index * MAX_RELATION_ID_OF_A_DIRECTBUFFER).toInt * DATA_LENGTH
    (index.toInt, offset)
  }

  def clear(): Unit ={
    directBufferPageArray.foreach(buf => buf.release())
    directBufferPageArray.clear()
  }
}

case class MyRelation(from: Long, to: Long, label: Int)

class NoRelationGetException extends Exception{
  override def getMessage: String = "no relation get"
}