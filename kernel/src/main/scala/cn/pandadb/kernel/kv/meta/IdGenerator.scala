package cn.pandadb.kernel.kv.meta

import java.util.concurrent.atomic.AtomicLong

import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.RocksDB


class IdGenerator(val db: KeyValueDB, val sequenceSize: Int) {

//  val keyBytes: Array[Byte]


  private val id: AtomicLong = {
    val iter = db.newIterator()
    iter.seekToLast()
    val value: Array[Byte] = iter.key()
    if (value == null) {
      new AtomicLong(0)
    }
    else {
      val current: Long = ByteUtils.getLong(value, 0)
      new AtomicLong(current)
    }
  }

  private val bound = new AtomicLong(0)

  private def slideDown(current: Long): Unit = {
    val end = current + sequenceSize - 1
//    writeId(end)
    bound.set(end)
  }

  private def slideDownIfNeeded(nid: Long): Unit = {
    if (nid > bound.get()) {
      slideDown(nid)
    }
  }

  def currentId() = id.get()

  def update(newId: Long): Unit = {
    if (newId < currentId()) {
      throw new LowerIdSetException(newId)
    }

    id.set(newId)
    slideDownIfNeeded(newId)
  }

  def nextId(): Long = {
    val nid = id.incrementAndGet()
    //all ids consumed
    slideDownIfNeeded(nid)
    nid
  }

  //save current id
//  def flush(): Unit = {
//    writeId(id.get())
//  }
//
//  private def writeId(num: Long): Unit = {
//    db.put(keyBytes, ByteUtils.longToBytes(num))
//  }
}

class LowerIdSetException(id: Long) extends RuntimeException(s"lower id set: $id") {

}
