package cn.pandadb.kernel.kv

import java.nio.ByteBuffer


import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class NodeIndex(val dbPath:String = "C:\\rocksDB"){
  //TODO 1.IndexId 8字节有所浪费 2.Index元数据怎么存,存哪些 3. 范围索引
  
  type IndexId   = Long
  type ValueType = Long
  type NodeId    = Long


  val db = RocksDBStorage.getDB(dbPath)


  /**
   * Index MetaData
   */
  def writeIndexMeta()= ???

  def deleteIndexMeta(IndexId: IndexId) = ???

  /**
   * Index Data
   */
  def writeIndexRow(indexId: IndexId, value: ValueType, nodeId: NodeId): Unit = {
    db.put(key(indexId, value, nodeId), Array.emptyByteArray) // TODO batch
  }

  def deleteSingleIndexRow(indexId: IndexId, value: ValueType, nodeId: NodeId) = {
    db.delete(key(indexId, value, nodeId))
  }

  def deleteIndexRows(indexId: IndexId) = {
    db.deleteRange(key(indexId, 0.toLong, 0.toLong), key(indexId, -1.toLong, -1.toLong))
  }


  /**
   * Index Data
   */
  def createIndex() = ???

  def dropIndex(indexId: IndexId) = {
    deleteIndexMeta(indexId)
    deleteIndexRows(indexId)
  }

  def find(indexId: IndexId, value: ValueType) = {
    val result = ListBuffer.empty[Long]
    val iter = db.newIterator()
    val prefix = key2(indexId, value)
    iter.seek(prefix)
    while(iter.isValid&&iter.key().startsWith(prefix)) {
      result += ByteBuffer.wrap(iter.key()).getLong(16)
      iter.next()
    }
    iter.close()
    result.toList
  }

  def findRange(indexId: IndexId, valueFrom: ValueType, valueTo: ValueType) = {

  }



  //key
  def key(a: Long, b: Long, c: Long) = {
    val bf = ByteBuffer.allocate(24)
    bf.putLong(a)
    bf.putLong(b)
    bf.putLong(c)
    bf.array()
  }

  def key2(a: Long, b: Long) = {
    val bf = ByteBuffer.allocate(16)
    bf.putLong(a)
    bf.putLong(b)
    bf.array()
  }
}
