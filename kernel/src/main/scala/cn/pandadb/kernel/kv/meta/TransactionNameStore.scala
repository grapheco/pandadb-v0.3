package cn.pandadb.kernel.kv.meta

import java.util.concurrent.atomic.AtomicInteger

import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.{Transaction, TransactionDB}

import scala.collection.mutable

trait TransactionNameStore {
  val db: TransactionDB
  val initInt: Int
  val key2ByteArrayFunc: (Int) => Array[Byte]
  val keyPrefixFunc: () => Array[Byte]

  var idGenerator: AtomicInteger = new AtomicInteger(initInt)
  var mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
  var mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()

  private def addToDB(labelName: String, tx: Transaction): Int = {
    // todo: reset map and id if failed.
    val id = idGenerator.incrementAndGet()
    mapString2Int += labelName -> id
    mapInt2String += id -> labelName
    val key = key2ByteArrayFunc(id)
    tx.put(key, ByteUtils.stringToBytes(labelName))
    id
  }

  def key(id: Int): Option[String] = mapInt2String.get(id)

  def id(labelName: String): Option[Int] = mapString2Int.get(labelName)

  def getOrAddId(labelName: String, tx: Transaction): Int =
    id(labelName).getOrElse(addToDB(labelName, tx))

  def ids(keys: Set[String], tx: Transaction): Set[Int] = {
    val newIds = keys.map {
      key =>
        val opt = mapString2Int.get(key)
        if (opt.isDefined) {
          opt.get
        }
        else {
          addToDB(key, tx)
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
    idGenerator = new AtomicInteger(initInt)
    mapString2Int = mutable.Map[String, Int]()
    mapInt2String = mutable.Map[Int, String]()
    var maxId: Int = initInt
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
