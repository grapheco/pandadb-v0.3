package cn.pandadb.kernel.direct

import java.nio.ByteBuffer

import cn.pandadb.kernel.store.StoredNode
import sun.nio.ch.DirectBuffer

import scala.collection.mutable

class DirectBufferArrayForNode(memorySize: Int, dataSize: Int) extends mutable.Iterable[StoredNode] {

  private val DATA_SIZE = dataSize
  private var MEMORY_SIZE = if (memorySize < 1024) 1024 else memorySize
  private var directBuffer = ByteBuffer.allocateDirect(MEMORY_SIZE)
  private var bufPos = 0 // global position of directBuffer, only put and delete can change it.
  private val deleteLog = mutable.Queue[Int]()

   def put(r: StoredNode): Unit = {
    // check deleteLog is empty, if nonEmpty put to that place.
    if (!deleteLog.isEmpty) {
      directBuffer.position(deleteLog.dequeue())
      directBuffer.putLong(r.id)
    }
    else {
      directBuffer.position(bufPos) // located position to write

      if (directBuffer.remaining() > DATA_SIZE) {
        directBuffer.putLong(r.id)
        bufPos = directBuffer.position()
      }
      // expand memory
      else {
        MEMORY_SIZE *= 2
        try {
          val tmp = ByteBuffer.allocateDirect(MEMORY_SIZE)
          directBuffer.flip()
          tmp.put(directBuffer)
          directBuffer.asInstanceOf[DirectBuffer].cleaner().clean()
          directBuffer = tmp
          directBuffer.putLong(r.id)
          bufPos = directBuffer.position()
        }
        catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

   def get(nodeId: Long): StoredNode = {
    var flag = true
    var index = 0
    directBuffer.position(index)
    while (directBuffer.getLong() != nodeId && flag) {
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
      StoredNode(directBuffer.getLong())
    } else null
  }

   def delete(nodeId: Long): Unit = {
    if (get(nodeId) != null) {
      deleteLog.enqueue(directBuffer.position() - DATA_SIZE)
    }
  }

   def update(r: StoredNode): Unit = {
    if (get(r.id) != null) {
      directBuffer.position(directBuffer.position() - DATA_SIZE)
      directBuffer.putLong(r.id)
    }
  }

   override def size(): Int = bufPos / DATA_SIZE

   def capacity(): Int = directBuffer.capacity()

  override def iterator: Iterator[StoredNode] = {
    new NodeIterator(directBuffer, bufPos, DATA_SIZE)
  }

  def clear(): Unit = {
    directBuffer.asInstanceOf[DirectBuffer].cleaner().clean()
  }
}

class NodeIterator(buffer: ByteBuffer, bufPos: Int, dataSize: Int) extends Iterator[StoredNode] {
  var index = 0
  var position = 0

  override def hasNext: Boolean = {
    if (index < bufPos) {
      index += dataSize
      true
    } else false
  }

  override def next(): StoredNode = {
    if (position < bufPos) {
      buffer.position(position)
      val res = StoredNode(buffer.getLong())
      position += dataSize
      res
    } else null
  }
}

