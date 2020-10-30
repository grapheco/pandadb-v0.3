package cn.pandadb.kernel.impl

import java.nio.ByteBuffer

import scala.collection.mutable

class ByteBufferArray(memorySize: Int) extends BasicOp {

  private var DATA_SIZE = 8 * 4
  private var MEMORY_SIZE = if (memorySize < 1024) 1024 else memorySize
  private var directBuffer = ByteBuffer.allocateDirect(MEMORY_SIZE)
  private var bufPos = 0 // global position of index of write data
  private val deleteLog = mutable.Queue[Int]()

  override def put(relationId: Long, typeId: Long, from: Long, to: Long): Unit = {
    // check deleteLog is empty, if nonEmpty put to that place.
    if (!deleteLog.isEmpty){
      directBuffer.position(deleteLog.dequeue())
      directBuffer.putLong(relationId).putLong(typeId).putLong(from).putLong(to)
    }
    else{
      directBuffer.position(bufPos) // located position to write

      if (directBuffer.remaining() > DATA_SIZE) {
        directBuffer.putLong(relationId).putLong(typeId).putLong(from).putLong(to)
        bufPos = directBuffer.position()
      }
      // expand memory
      else {
        MEMORY_SIZE *= 2
        try {
          val tmp = ByteBuffer.allocate(MEMORY_SIZE)
          directBuffer.flip()
          tmp.put(directBuffer)
          directBuffer = tmp
          directBuffer.putLong(relationId).putLong(typeId).putLong(from).putLong(to)
          bufPos = directBuffer.position()
        }
        catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  override def get(relationId: Long): (Int, (Long, Long, Long, Long)) = {
    var flag = true
    var index = 0
    directBuffer.position(index)
    while (directBuffer.getLong() != relationId && flag) {
      index += DATA_SIZE
      if (index < directBuffer.capacity()) {
        directBuffer.position(index)
      }
      else {
        flag = false
      }
    }
    // found
    if (flag) {
      directBuffer.position(index)
      (index/DATA_SIZE, (directBuffer.getLong(), directBuffer.getLong(), directBuffer.getLong(), directBuffer.getLong()))
    } else null
  }

  override def delete(relationId: Long): Unit = {
    if (get(relationId) != null) {
      deleteLog.enqueue(directBuffer.position() - DATA_SIZE)
    }
  }

  override def update(relationId: Long, typeId: Long, from: Long, to: Long): Unit = {
    if (get(relationId) != null) {
      directBuffer.position(directBuffer.position() - DATA_SIZE)
      directBuffer.putLong(relationId)
      directBuffer.putLong(typeId)
      directBuffer.putLong(from)
      directBuffer.putLong(to)
    }
  }

  override def size(): Int = bufPos / DATA_SIZE

  override def capacity(): Int = directBuffer.capacity()
}

