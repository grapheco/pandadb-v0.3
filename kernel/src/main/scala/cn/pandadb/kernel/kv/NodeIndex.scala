package cn.pandadb.kernel.kv

import java.nio.ByteBuffer


import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class NodeIndex(val dbPath:String = "C:\\rocksDB"){
  //TODO  2.Index元数据怎么存,存哪些 3. 范围索引
  
  type IndexId   = Int
  type ValueType = Long
  type NodeId    = Long

  val db = RocksDBStorage.getDB(dbPath)
  /**
   * Index MetaData
   * indexId, label, prop
   */
  def writeIndexMeta(): Unit = {

  }

  def deleteIndexMeta(IndexId: IndexId): Unit = {

  }

  /**
   * Index Data
   */
  def writeIndexRow(indexId: IndexId, value: ValueType, nodeId: NodeId): Unit = {
    db.put(KeyHandler.nodePropertyIndexKeyToBytes(indexId, value, nodeId), Array.emptyByteArray) // TODO batch
  }

  def deleteSingleIndexRow(indexId: IndexId, value: ValueType, nodeId: NodeId): Unit = {
    db.delete(KeyHandler.nodePropertyIndexKeyToBytes(indexId, value, nodeId))
  }

  def deleteIndexRows(indexId: IndexId): Unit = {
    db.deleteRange(KeyHandler.nodePropertyIndexKeyToBytes(indexId, 0.toLong, 0.toLong),
      KeyHandler.nodePropertyIndexKeyToBytes(indexId, -1.toLong, -1.toLong))
  }

  def updateIndexRow(indexId: IndexId, value: ValueType, nodeId: NodeId, newValue: ValueType): Unit = {
    deleteSingleIndexRow(indexId, value, nodeId)
    writeIndexRow(indexId, newValue, nodeId)
  }

  /**
   * Index Data
   */
  def createIndex() = ???

  def dropIndex(indexId: IndexId): Unit = {
    deleteIndexMeta(indexId)
    deleteIndexRows(indexId)
  }

  def find(indexId: IndexId, value: ValueType): List[NodeId] = {
    val result = ListBuffer.empty[Long]
    val iter = db.newIterator()
    val prefix = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, value)
    iter.seek(prefix)
    while(iter.isValid&&iter.key().startsWith(prefix)) {
      result += ByteBuffer.wrap(iter.key()).getLong(16)
      iter.next()
    }
    iter.close()
    result.toList
  }

  // TODO 有问题
  def findRange(indexId: IndexId, valueFrom: ValueType, valueTo: ValueType): Unit = {
  }

}
