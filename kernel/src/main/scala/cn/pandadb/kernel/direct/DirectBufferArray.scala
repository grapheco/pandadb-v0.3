package cn.pandadb.kernel.direct

import java.nio.ByteBuffer

import cn.pandadb.kernel.store.StoredRelation

import scala.collection.mutable

class DirectBufferArray(memorySize: Int, dataSize: Int) extends BasicOp with mutable.Iterable[StoredRelation] {

  private val DATA_SIZE = dataSize
  private var MEMORY_SIZE = if (memorySize < 1024) 1024 else memorySize
  private var directBuffer = ByteBuffer.allocateDirect(MEMORY_SIZE)
  private var bufPos = 0 // global position of directBuffer, only put and delete can change it.
  private val deleteLog = mutable.Queue[Int]()

  override def put(r: StoredRelation): Unit = {
    // check deleteLog is empty, if nonEmpty put to that place.
    if (!deleteLog.isEmpty) {
      directBuffer.position(deleteLog.dequeue())
      directBuffer.putLong(r.id).putLong(r.from).putLong(r.to).putInt(r.labelId)
    }
    else {
      directBuffer.position(bufPos) // located position to write

      if (directBuffer.remaining() > DATA_SIZE) {
        directBuffer.putLong(r.id).putLong(r.from).putLong(r.to).putInt(r.labelId)
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
          directBuffer.putLong(r.id).putLong(r.from).putLong(r.to).putInt(r.labelId)
          bufPos = directBuffer.position()
        }
        catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  override def get(relationId: Long): StoredRelation = {
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
      StoredRelation(directBuffer.getLong(), directBuffer.getLong(), directBuffer.getLong(), directBuffer.getInt())
    } else null
  }

  override def delete(relationId: Long): Unit = {
    if (get(relationId) != null) {
      deleteLog.enqueue(directBuffer.position() - DATA_SIZE)
    }
  }

  override def update(r: StoredRelation): Unit = {
    if (get(r.id) != null) {
      directBuffer.position(directBuffer.position() - DATA_SIZE)
      directBuffer.putLong(r.id)
      directBuffer.putLong(r.from)
      directBuffer.putLong(r.to)
      directBuffer.putInt(r.labelId)
    }
  }

  override def size(): Int = bufPos / DATA_SIZE

  override def capacity(): Int = directBuffer.capacity()

  override def iterator: Iterator[StoredRelation] = {
    new RelationIterator(directBuffer, bufPos, DATA_SIZE)
  }

  def clear(): Unit = {
    bufPos = 0
  }
}

class RelationIterator(buffer: ByteBuffer, bufPos: Int, dataSize: Int) extends Iterator[StoredRelation] {
  var index = 0
  var position = 0

  override def hasNext: Boolean = {
    if (index < bufPos) {
      index += dataSize
      true
    } else false
  }

  override def next(): StoredRelation = {
    if (position < bufPos) {
      buffer.position(position)
      val res = StoredRelation(buffer.getLong(), buffer.getLong(), buffer.getLong(), buffer.getInt())
      position += dataSize
      res
    } else null
  }
}

