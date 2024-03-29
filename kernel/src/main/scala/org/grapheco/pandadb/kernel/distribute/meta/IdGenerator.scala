package org.grapheco.pandadb.kernel.distribute.meta

import java.util.concurrent.atomic.AtomicLong

import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
/**
 * 1. write: first update to tikv, then use it
 */
class IdGenerator(db: DistributedKVAPI, idType: TypeNameEnum.Value) {
  val key = {
    idType match {
      case TypeNameEnum.nodeName => DistributedKeyConverter.nodeMaxIdKey
      case TypeNameEnum.relationName => DistributedKeyConverter.relationMaxIdKey
    }
  }


  private val id: AtomicLong = {
    val res = db.get(key)
    if (res.isEmpty) new AtomicLong(0)
    else new AtomicLong(ByteUtils.getLong(res, 0))
  }

  def refreshId(): Unit ={
    val newId = {
      val res = db.get(key)
      if (res.isEmpty) new AtomicLong(0)
      else new AtomicLong(ByteUtils.getLong(res, 0))
    }
    id.set(newId.get())
  }

  def currentId() = id.get()

  def resetId() = {
    id.set(0)
    flushId()
  }

  def nextId(): Long = {
    val nid = id.incrementAndGet()
    flushId()
    nid
  }

  def flushId(): Unit ={
    db.put(key, ByteUtils.longToBytes(id.get()))
  }

}
