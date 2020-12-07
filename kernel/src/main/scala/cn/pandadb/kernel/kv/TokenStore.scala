package cn.pandadb.kernel.kv

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import org.rocksdb.{RocksDB}

trait TokenStore{
  val db: RocksDB
  val key2ByteArrayFunc: (Int) => Array[Byte]
  val keyPrefixFunc: () => Array[Byte]

  val idGenerator: AtomicInteger = new AtomicInteger(0)
  val mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
  val mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()

  def addToDB(labelName: String): Int ={
    val id = idGenerator.incrementAndGet()
    mapString2Int += labelName -> id
    mapInt2String += id -> labelName
    val key = key2ByteArrayFunc(id)
    db.put(key, ByteUtils.stringToBytes(labelName))
    id
  }

  def set(labelName: String): Int ={
    val opt = mapString2Int.get(labelName)
    if (opt.isDefined){
      mapString2Int(labelName)
    }
    else{
      addToDB(labelName)
    }
  }

  def key(id: Int): Option[String] ={
    mapInt2String.get(id)
  }
  def id(labelName: String): Int ={
    val opt = mapString2Int.get(labelName)
    if (opt.isDefined){
      opt.get
    }
    else{
      addToDB(labelName)
    }
  }
  def ids(keys:Set[String]): Set[Int] ={
    val newIds = keys.map{
      key =>
        val opt = mapString2Int.get(key)
        if (opt.isDefined){
          opt.get
        }
        else{
          val id = idGenerator.incrementAndGet()
          mapString2Int += key -> id
          mapInt2String += id -> key
          val dbKey = key2ByteArrayFunc(id)
          db.put(dbKey, ByteUtils.stringToBytes(key))
          id
        }
    }
    newIds
  }

  def delete(labelName: String): Unit ={
    val id = mapString2Int(labelName)
    mapString2Int -= labelName
    mapInt2String -= id
    val key = key2ByteArrayFunc(id)
    db.delete(key)
  }

  def loadAll(): Unit ={
    var maxId: Int = 0
    val prefix = keyPrefixFunc()
    val iter = db.newIterator()
    iter.seek(prefix)

    while (iter.isValid && iter.key().startsWith(prefix)){
      val id = ByteUtils.getInt(iter.key(), 1)
      if (maxId < id) maxId = id
      val name = ByteUtils.stringFromBytes(db.get(iter.key()))
      mapString2Int += name -> id
      mapInt2String += id -> name
      iter.next()
    }
    idGenerator.set(maxId)
  }

//  def saveAll(): Unit ={
//    val writeOptions = new WriteOptions()
//    val batch = new WriteBatch()
//    mapInt2String.foreach(f => {
//      val key = key2ByteArrayFunc(f._1)
//      batch.put(key, ByteUtils.stringToBytes(f._2))
//    })
//    db.write(writeOptions, batch)
//  }

  def close(): Unit ={
    db.close()
  }

}
