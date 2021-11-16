package cn.pandadb.kernel.distribute.meta

import java.util.concurrent.atomic.AtomicLong

import cn.pandadb.kernel.distribute.DistributedKVAPI
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
/**
 * 1. write: first update to tikv, then use it
 */
class IdGenerator(db: DistributedKVAPI, idType: TypeNameEnum.Value) {
  val key = {
    idType match {
      case TypeNameEnum.nodeName => KeyConverter.nodeMaxIdKey
      case TypeNameEnum.relationName => KeyConverter.relationMaxIdKey
    }
  }


  private val id: AtomicLong = {
    val res = db.get(key)
    if (res.isEmpty) new AtomicLong(0)
    else new AtomicLong(ByteUtils.getLong(res, 0))
  }


  def currentId() = id.get()


  def nextId(): Long = {
    val nid = id.incrementAndGet()
    //all ids consumed ?
    nid
  }

  def flushId(): Unit ={
    db.put(key, ByteUtils.longToBytes(id.get()))
  }

}
