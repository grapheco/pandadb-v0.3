package cn.pandadb.kernel.kv.meta

import java.util.concurrent.atomic.AtomicInteger

import cn.pandadb.kernel.kv.ByteUtils
import org.rocksdb.RocksDB

import scala.collection.mutable

trait NameStore {
  val db: RocksDB
  val key2ByteArrayFunc: (Int) => Array[Byte]
  val keyPrefixFunc: () => Array[Byte]

  val idGenerator: AtomicInteger = new AtomicInteger(0)
  val mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
  val mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()

  private def addToDB(labelName: String): Int = {
    val id = idGenerator.incrementAndGet()
    mapString2Int += labelName -> id
    mapInt2String += id -> labelName
    val key = key2ByteArrayFunc(id)
    db.put(key, ByteUtils.stringToBytes(labelName))
    id
  }

  def key(id: Int): Option[String] = {
    mapInt2String.get(id)
  }

  def id(labelName: String): Int = {
    val opt = mapString2Int.get(labelName)
    if (opt.isDefined) {
      opt.get
    }
    else {
      addToDB(labelName)
    }
  }

  def ids(keys: Set[String]): Set[Int] = {
    val newIds = keys.map {
      key =>
        val opt = mapString2Int.get(key)
        if (opt.isDefined) {
          opt.get
        }
        else {
          addToDB(key)
        }
    }
    newIds
  }

  def delete(labelName: String): Unit = {
    val id = mapString2Int(labelName)
    mapString2Int -= labelName
    mapInt2String -= id
    val key = key2ByteArrayFunc(id)
    db.delete(key)
  }

  def loadAll(): Unit = {
    var maxId: Int = 0
    val prefix = keyPrefixFunc()
    val iter = db.newIterator()
    iter.seek(prefix)

    while (iter.isValid && iter.key().startsWith(prefix)) {
      val key = iter.key()
      val id = ByteUtils.getInt(key, 1)
      if (maxId < id) maxId = id
      val name = ByteUtils.stringFromBytes(db.get(key))
      mapString2Int += name -> id
      mapInt2String += id -> name
      iter.next()
    }
    idGenerator.set(maxId)
  }

  def close(): Unit = {
    db.close()
  }

}
