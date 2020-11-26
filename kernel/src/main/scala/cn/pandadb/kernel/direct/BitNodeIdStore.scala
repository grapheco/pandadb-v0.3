package cn.pandadb.kernel.direct

import io.netty.buffer.{ByteBuf, Unpooled}

import scala.collection.mutable.ArrayBuffer

// 2GB = 4.29 billion nodeId
class BitNodeIdStore(maxCapacityBytes: Int = Unpooled.directBuffer().maxCapacity()) {
  var MAX_CAPACITY = maxCapacityBytes
  val directBufferArray = new ArrayBuffer[ByteBuf]()
  val MAX_NUM_OF_A_BUFFER = MAX_CAPACITY.toLong << 3
  val bitUtil = new BitUtil

  private def mallocDirectBuffer(): Unit = {
    val directBuffer = Unpooled.directBuffer(MAX_CAPACITY)
    directBuffer.setZero(0, MAX_CAPACITY)
    directBufferArray.append(directBuffer)
  }

  def setNodeId(nodeId: Long): Unit = {
    var directBuffer: ByteBuf = null
    var offset: Long = 0

    val index = (nodeId - 1) / MAX_NUM_OF_A_BUFFER
    if (index + 1 > directBufferArray.length) {
      val distance = index + 1 - directBufferArray.length
      for (i <- 1 to distance.toInt) {
        mallocDirectBuffer()
      }
      directBuffer = directBufferArray.last
      offset = ((nodeId - 1) - (directBufferArray.length - 1) * MAX_NUM_OF_A_BUFFER) >> 6 << 3
    }
    else {
      directBuffer = directBufferArray(index.toInt)
      offset = ((nodeId - 1) - index * MAX_NUM_OF_A_BUFFER) >> 6 << 3
    }
    bitUtil.setBit(directBuffer, offset.toInt, nodeId)
  }

  def exists(nodeId: Long): Boolean = {
    val position = getPosition(nodeId)
    bitUtil.exists(position._1, position._2, nodeId)
  }

  def reset(nodeId: Long): Unit = {
    val position = getPosition(nodeId)
    bitUtil.reset(position._1, position._2, nodeId)
  }

  def getAllNodeId(): Iterator[Long] ={
    new NodeIterator(this)
  }

  def getPosition(nodeId: Long): (ByteBuf, Int) = {
    var directBuffer: ByteBuf = null
    var offset: Long = 0
    val index = (nodeId - 1) / MAX_NUM_OF_A_BUFFER
    directBuffer = directBufferArray(index.toInt)
    offset = ((nodeId - 1) - index * MAX_NUM_OF_A_BUFFER) >> 6 << 3

    (directBuffer, offset.toInt)
  }
}

class NodeIterator(store: BitNodeIdStore) extends Iterator[Long]{
  var array = store.directBufferArray
  var buffer: ByteBuf = null
  val util = new BitUtil
  var arrayCount = 0
  var nodeId: Long = 0
  var numCount: Long = 0

  if (array.nonEmpty){
    buffer = array(0)
  }
  else {
    throw new NoNextNodeIdException
  }

  override def hasNext: Boolean = {
    var isFinish: Boolean = false
    var isFound: Boolean = false

    while (!isFinish){
      if (arrayCount < array.length){
        if (numCount < store.MAX_NUM_OF_A_BUFFER){
          numCount += 1
          nodeId += 1
          val index = (nodeId - 1) / store.MAX_NUM_OF_A_BUFFER
          val offset = ((nodeId - 1) - index * store.MAX_NUM_OF_A_BUFFER) >> 6 << 3
          val find = util.exists(buffer, offset.toInt, nodeId)
          if (find){
            isFinish = true
            isFound = true
          }
        }
        else{
          arrayCount += 1
          numCount = 0
          if (arrayCount < array.length) buffer = array(arrayCount)
        }
      }
      else{
        isFinish = true
        isFound = false
      }
    }
    isFound
  }

  override def next(): Long = nodeId
}

class BitUtil {
  // % 0X3F = % 64 (0~63)
  def setBit(directBuffer: ByteBuf, offset: Int, nodeId: Long): Unit = {
    val value = directBuffer.getLong(offset)
    val toSet = value | (1.toLong << (nodeId & 0X3F))
    directBuffer.setLong(offset, toSet)
  }

  def exists(directBuffer: ByteBuf, offset: Int, nodeId: Long): Boolean = {
    val value = directBuffer.getLong(offset)
    (value & (1.toLong << (nodeId & 0X3F))) != 0
  }

  def reset(directBuffer: ByteBuf, offset: Int, nodeId: Long): Unit = {
    val value = directBuffer.getLong(offset)
    val toSet = value & (~(1.toLong << (nodeId & 0X3F)))
    directBuffer.setLong(offset, toSet)
  }
}

class NoNextNodeIdException extends Exception{
  override def getMessage: String = "next on empty iterator"
}