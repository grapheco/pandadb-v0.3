package cn.pandadb.kernel.kv

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import cn.pandadb.kernel.kv.KeyHandler.KeyType
import org.rocksdb.{RocksDB, RocksIterator}

class RelationStore(db: RocksDB) {

  def map2ArrayByte(map: Map[String, Object]): Array[Byte] ={
    val os = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(os)
    oos.writeObject(map)
    val bt = os.toByteArray
    oos.close()
    os.close()
    bt
  }
  def arrayByte2Map(array: Array[Byte]): Map[String, Object] ={
    val bis = new ByteArrayInputStream(array)
    val ois = new ObjectInputStream(bis)
    val map = ois.readObject().asInstanceOf[Map[String, Object]]
    map
  }

  def putRelation(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long, valueMap: Map[String, Object]): Unit ={
    val inKey = KeyHandler.inEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val outKey = KeyHandler.outEdgeKeyToBytes(toNodeId, fromNodeId, labelId, category)
    val value = map2ArrayByte(valueMap)

    db.put(inKey, value)
    db.put(outKey, value)
  }

  def getRelationValueMap(key:Array[Byte]): Map[String, Object] ={
    val mapBytes = db.get(key)
    arrayByte2Map(mapBytes)
  }

  def relationIsExist(key:Array[Byte]): Boolean ={
    val res = db.get(key)
    if (res == null) false else true
  }

  def updateRelation(key:Array[Byte], valueMap: Map[String, Object]): Unit ={
    val redundancy: Array[Byte] = KeyHandler.twinEdgeKey(key)
    val value = map2ArrayByte(valueMap)

    db.put(key, value)
    db.put(redundancy, value)
  }

  def deleteRelation(key:Array[Byte]): Unit ={
    val redundancy: Array[Byte] = KeyHandler.twinEdgeKey(key)
    db.delete(key)
    db.delete(redundancy)
  }

  def getAllRelation(relationType: Byte, startNodeId: Long): Iterator[Array[Byte]] ={
    new GetAllRocksRelation(db, relationType, startNodeId)
  }

  def close(): Unit ={
    db.close()
  }

  class GetAllRocksRelation(db: RocksDB, relationType: Byte, startNodeId: Long) extends Iterator[Array[Byte]]{
    val prefix = new Array[Byte](9)
    ByteUtils.setByte(prefix, 0, relationType)
    ByteUtils.setLong(prefix, 1, startNodeId)
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = {
      iter.isValid
    }

    override def next(): Array[Byte] = {
      val res = iter.key()
      iter.next()
      res
    }
  }
}
