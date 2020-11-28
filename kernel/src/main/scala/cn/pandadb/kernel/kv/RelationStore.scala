package cn.pandadb.kernel.kv

import cn.pandadb.kernel.kv.KeyHandler.KeyType
import com.alibaba.fastjson.{JSON, JSONObject}

class RelationStore {
  val db = RocksDBStorage.getDB()

  def writeRelation(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long, values: String): Unit = {
    val inKey = KeyHandler.inEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val outKey = KeyHandler.outEdgeKeyToBytes(toNodeId, fromNodeId, labelId, category)
    val value = JSON.toJSONBytes(values)

    db.put(inKey, value)
    db.put(outKey, value)
  }

  def getRelationValueObject(key: Array[Byte]): JSONObject = {
    val jsonBytes = db.get(key)
    var jsonString = new String(jsonBytes)
    jsonString = jsonString.slice(1, jsonBytes.length - 1).replaceAll("////", "")
    JSON.parseObject(jsonString)
  }

  def relationIsExist(key: Array[Byte]): Boolean = {
    val res = db.get(key)
    if (res == null) false else true
  }

  def updateRelation(key: Array[Byte], newValue: Array[Byte]): Unit = {
    db.put(key, newValue)
  }

  def deleteRelation(key: Array[Byte]): Unit = {
    // delete InEdge and OutEdge
    val rType = ByteUtils.getByte(key, 1)
    var redundancy: Array[Byte] = rType match {
      case KeyType.InEdge.id.toByte => {
        val fromNode = ByteUtils.getLong(key, 1)
        val label = ByteUtils.getInt(key, 9)
        val category = ByteUtils.getLong(key, 13)
        val toNode = ByteUtils.getLong(key, 21)
        KeyHandler.outEdgeKeyToBytes(fromNode, toNode, label, category)
      }
      case KeyType.OutEdge.id.toByte => {
        val fromNode = ByteUtils.getLong(key, 21)
        val label = ByteUtils.getInt(key, 9)
        val category = ByteUtils.getLong(key, 13)
        val toNode = ByteUtils.getLong(key, 1)
        KeyHandler.inEdgeKeyToBytes(fromNode, toNode, label, category)
      }
    }
    db.delete(key)
    db.delete(redundancy)
  }

  def getAll(relationType: Byte, startNodeId: Long): Iterator[Array[Byte]] = {
    new GetAllRocksRelation(relationType, startNodeId)
  }

  class GetAllRocksRelation(relationType: Byte, startNodeId: Long) extends Iterator[Array[Byte]]{
    val prefix = new Array[Byte](9)
    ByteUtils.setByte(prefix, 0, relationType)
    ByteUtils.setLong(prefix, 1, startNodeId)
    val rocksIterator = db.newIterator()

    override def hasNext: Boolean = {
      rocksIterator.isValid
    }

    override def next(): Array[Byte] = {
      val key = rocksIterator.key()
      rocksIterator.next()
      key
    }
  }
}
