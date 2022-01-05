package cn.pandadb.kernel.distribute.meta

import java.util.concurrent.atomic.AtomicInteger

import cn.pandadb.kernel.distribute.DistributedKVAPI
import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.udp.UDPClient

import scala.collection.mutable

trait DistributedNameStore {
  val db: DistributedKVAPI
  val initInt: Int
  val key2ByteArrayFunc: (Int) => Array[Byte]
  val keyPrefixFunc: () => Array[Byte]
  val udpClients: Array[UDPClient]

  var idGenerator: AtomicInteger = new AtomicInteger(initInt)
  var mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
  var mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()

  private def addToDB(labelName: String): Int = {
    val id = idGenerator.incrementAndGet()
    mapString2Int += labelName -> id
    mapInt2String += id -> labelName
    val key = key2ByteArrayFunc(id)
    db.put(key, ByteUtils.stringToBytes(labelName))

    udpClients.foreach(client => client.sendRefreshMsg())

    id
  }

  def key(id: Int): Option[String] = mapInt2String.get(id)

  def id(labelName: String): Option[Int] = mapString2Int.get(labelName)

  def existKey(labelName: String): Boolean = mapString2Int.contains(labelName)

  def existId(id: Int): Boolean = mapInt2String.contains(id)

  def getOrAddId(labelName: String): Int =
    id(labelName).getOrElse(addToDB(labelName))

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

    udpClients.foreach(client => client.sendRefreshMsg())
  }

  def cleanData(): Unit ={
    idGenerator = new AtomicInteger(initInt)
    mapString2Int = mutable.Map[String, Int]()
    mapInt2String = mutable.Map[Int, String]()
  }

  def loadAll(): Unit = {
    idGenerator = new AtomicInteger(initInt)
    mapString2Int = mutable.Map[String, Int]()
    mapInt2String = mutable.Map[Int, String]()
    var maxId: Int = initInt
    val prefix = keyPrefixFunc()

    val iter = db.scanPrefix(prefix, 10000, false)
    while (iter.hasNext){
      val key = iter.next().getKey.toByteArray
      val id = ByteUtils.getInt(key, 1)
      if (maxId < id) maxId = id
      val name = ByteUtils.stringFromBytes(db.get(key))
      mapString2Int += name -> id
      mapInt2String += id -> name
    }
    idGenerator.set(maxId)
  }

  def refreshNameStore(): Unit ={
    val tmpMapString2Int = mutable.Map[String, Int]()
    val tmpMapInt2String = mutable.Map[Int, String]()
    val prefix = keyPrefixFunc()
    var maxId: Int = initInt
    val iter = db.scanPrefix(prefix, 10000, false)
    while (iter.hasNext){
      val key = iter.next().getKey.toByteArray
      val id = ByteUtils.getInt(key, 1)
      if (maxId < id) maxId = id
      val name = ByteUtils.stringFromBytes(db.get(key))
      tmpMapString2Int += name -> id
      tmpMapInt2String += id -> name
    }
    mapString2Int = tmpMapString2Int
    mapInt2String = tmpMapInt2String
    idGenerator.set(maxId)
  }
}
