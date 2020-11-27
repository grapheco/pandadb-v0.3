package cn.pandadb.kernel.kv

import cn.pandadb.kernel.kv.KeyHandler.KeyType
import com.alibaba.fastjson.{JSON, JSONObject}
import org.rocksdb.RocksIterator

class RelationStore {
  val db = RocksDBStorage.getDB()

  def writeRelation(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long, values: String): Unit ={
    val inKey = KeyHandler.inEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val outKey = KeyHandler.outEdgeKeyToBytes(toNodeId, fromNodeId, labelId, category)
    val value = JSON.toJSONBytes(values)

    db.put(inKey, value)
    db.put(outKey, value)
  }

  def getRelationValueObject(key:Array[Byte]): JSONObject ={
    val jsonBytes = db.get(key)
    var jsonString = new String(jsonBytes)
    jsonString = jsonString.slice(1, jsonBytes.length - 1).replaceAll("////", "")
    JSON.parseObject(jsonString)
  }

  def relationIsExist(key:Array[Byte]): Boolean ={
    val res = db.get(key)
    if (res == null) false else true
  }

  def updateRelation(key:Array[Byte], newValue:Array[Byte]): Unit ={
    db.put(key, newValue)
  }

  def deleteRelation(key:Array[Byte]): Unit ={
    // delete outgoing and incoming
    val rtype = ByteUtils.getByte(key, 1)
    var redundancy: Array[Byte] = null
    rtype match {
      case KeyType.InEdge.id.toByte =>{
        val fromNode = ByteUtils.getLong(key, 1)
        val label = ByteUtils.getInt(key, 9)
        val category = ByteUtils.getLong(key, 13)
        val toNode =  ByteUtils.getLong(key, 21)
        redundancy = KeyHandler.outEdgeKeyToBytes(fromNode, toNode, label, category)
      }
      case KeyType.OutEdge.id.toByte =>{
        val fromNode = ByteUtils.getLong(key, 21)
        val label = ByteUtils.getInt(key, 9)
        val category = ByteUtils.getLong(key, 13)
        val toNode =  ByteUtils.getLong(key, 1)
        redundancy = KeyHandler.inEdgeKeyToBytes(fromNode, toNode, label, category)
      }
    }
    db.delete(key)
    db.delete(redundancy)
  }

  def getAllRelation(relationType: Byte, startNodeId: Long): (Array[Byte], RocksIterator) ={
    val prefix = new Array[Byte](9)
    ByteUtils.setByte(prefix, 0, relationType)
    ByteUtils.setLong(prefix, 1, startNodeId)
    val iter = db.newIterator()
    iter.seek(prefix)
    (prefix, iter)
  }
}
