package cn.pandadb.kernel.kv

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

trait TokenStore{
  val db: RocksDB
  val key2ByteArrayFunc: (Int) => Array[Byte]
  val keyPrefixFunc: () => Array[Byte]

  val idGenerator: AtomicInteger = new AtomicInteger(0)
  val mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
  val mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()

  def set(labelName: String): Unit ={
    if (!mapString2Int.keySet.contains(labelName)){
      val id = idGenerator.incrementAndGet()
      mapString2Int += labelName -> id
      mapInt2String += id -> labelName
    }
  }

  def key(id: Int): String ={
    mapInt2String(id)
  }
  def id(labelName: String): Int ={
    mapString2Int(labelName)
  }
  def ids(): Set[Int] ={
    mapInt2String.keySet.toSet
  }

  def delete(labelName: String): Unit ={
    val id = mapString2Int(labelName)
    mapString2Int -= labelName
    mapInt2String -= id
  }

  def loadAll(): Unit ={
    var maxId: Int = 0
    val iter = db.newIterator()
    iter.seek(keyPrefixFunc())

    while (iter.isValid && iter.key().startsWith(keyPrefixFunc())){
      val id = ByteUtils.getInt(iter.key(), 1)
      if (maxId < id) maxId = id
      val name = ByteUtils.stringFromBytes(db.get(iter.key()))
      mapString2Int += name -> id
      mapInt2String += id -> name
      iter.next()
    }
    idGenerator.set(maxId)
  }

  def saveAll(): Unit ={
    val writeOptions = new WriteOptions()
    val batch = new WriteBatch()
    mapInt2String.foreach(f => {
      val key = key2ByteArrayFunc(f._1)
      batch.put(key, ByteUtils.stringToBytes(f._2))
    })
    db.write(writeOptions, batch)
  }

  def close(): Unit ={
    db.close()
  }
}
