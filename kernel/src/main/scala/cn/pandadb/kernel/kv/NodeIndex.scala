package cn.pandadb.kernel.kv

import java.nio.ByteBuffer

import org.rocksdb.RocksDB

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

class NodeIndex(db: RocksDB){
  
  type IndexId   = Int
  type ValueType = Long
  type NodeId    = Long

  /**
   * Index MetaData
   * ------------------------
   *      key      |  value
   * ------------------------
   * label + props |  indexId
   * ------------------------
   */
  def addIndexMeta(label: Int, props: Array[Int]): IndexId = {
    val key = KeyHandler.nodePropertyIndexInfoToBytes(label, props)
    val id  = db.get(key)
    if (id == null || id.length == 0){
      val new_id = Random.nextInt(100) // TODO generate
      db.put(key, Array[Byte](new_id.toByte))
      new_id
    } else {
      // exist
      ByteUtils.getInt(id, 0)
    }
  }

  def deleteIndexMeta(label: Int, props: Array[Int]): Unit = {
    db.delete(KeyHandler.nodePropertyIndexInfoToBytes(label, props))
  }

  def getIndexId(label: Int, props: Array[Int]): IndexId = {
    val v = db.get(KeyHandler.nodePropertyIndexInfoToBytes(label, props))
    if (v == null || v.length < 4) {
      -1
    }else{
      ByteUtils.getInt(v, 0)
    }
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
  def createIndex(label: Int, props: Array[Int]) = {
    addIndexMeta(label, props)
  }

  def dropIndex(label: Int, props: Array[Int]): Unit = {
    deleteIndexRows(getIndexId(label, props))
    deleteIndexMeta(label, props)
  }

  def find(indexId: IndexId, value: ValueType): Iterator[NodeId] = {
    val iter = db.newIterator()
    val prefix = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, value)
    iter.seek(prefix)
    new Iterator[NodeId] (){
      override def hasNext: Boolean = iter.isValid() && iter.key().startsWith(prefix)

      override def next(): NodeId = {
        val id = ByteBuffer.wrap(iter.key()).getLong(13)
        iter.next()
        id
      }
    }
  }

  // TODO 有问题
  def findRange(indexId: IndexId, valueFrom: ValueType, valueTo: ValueType): Unit = {
  }

}
