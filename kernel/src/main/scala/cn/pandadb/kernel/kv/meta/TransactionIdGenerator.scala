package cn.pandadb.kernel.kv.meta

import java.util.concurrent.atomic.AtomicLong

import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.util.log.PandaLog
import org.rocksdb.TransactionDB

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-09 17:32
 */
class TransactionIdGenerator(val db: TransactionDB, val sequenceSize: Int, logWriter: PandaLog) {
  private val id: AtomicLong = {
    val iter = db.newIterator()
    iter.seekToLast()
    val value: Array[Byte] = iter.key()
    if (value == null) {
      new AtomicLong(0)
    }
    else {
      val current: Long = if (value.length<8) 0L else ByteUtils.getLong(value, 0)
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
}

