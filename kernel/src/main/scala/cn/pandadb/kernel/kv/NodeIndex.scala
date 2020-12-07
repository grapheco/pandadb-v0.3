package cn.pandadb.kernel.kv

import java.nio.ByteBuffer

import cn.pandadb.kernel.kv.KeyHandler.KeyType
import cn.pandadb.kernel.kv.NodeIndex.{IndexId, NodeId, metaIdKey}
import org.rocksdb.RocksDB

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object NodeIndex{
  type IndexId   = Int
  type NodeId    = Long
  val metaIdKey = Array[Byte](KeyType.NodePropertyIndexMeta.id.toByte)
}

class NodeIndex(db: RocksDB){
  


  /**
   * Index MetaData
   * ------------------------
   *      key      |  value
   * ------------------------
   * label + props |  indexId
   * ------------------------
   */
  def addIndexMeta(label: Int, props: Array[Int]): IndexId = {
    val key = KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props)
    val id  = db.get(key)
    if (id == null || id.isEmpty){
      val new_id = getIncreasingId()
      val id_byte = new Array[Byte](4)
      ByteUtils.setInt(id_byte, 0, new_id)
      db.put(key,id_byte)
      new_id
    } else {
      // exist
      ByteUtils.getInt(id, 0)
    }
  }

  def getIncreasingId(): IndexId ={
    val increasingId = db.get(metaIdKey)
    val id_bytes = new Array[Byte](4)
    if (increasingId == null || increasingId.length == 0){
      val id:Int = 0
      ByteUtils.setInt(id_bytes,0, id+1)
      db.put(metaIdKey, id_bytes)
      id
    } else {
      val id:Int = ByteBuffer.wrap(increasingId).getInt(0)
      ByteUtils.setInt(id_bytes,0, id+1)
      db.put(metaIdKey, id_bytes)
      id
    }
  }

  def deleteIndexMeta(label: Int, props: Array[Int]): Unit = {
    db.delete(KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props))
  }

  def getIndexId(label: Int, props: Array[Int]): IndexId = {
    val v = db.get(KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props))
    if (v == null || v.length < 4) {
      -1
    }else{
      ByteUtils.getInt(v, 0)
    }
  }

  def getIndexId(label: Int, prop: Int): IndexId = {
    getIndexId(label, Array[Int](prop))
  }

  /**
   * Index Data
   */
  private def writeIndexRow(indexId: IndexId, value: Array[Byte], length: Array[Byte], nodeId: NodeId): Unit = {
    db.put(KeyHandler.nodePropertyIndexKeyToBytes(indexId, value, length, nodeId), Array.emptyByteArray)
  }

  private def writeIndexRow(indexId: IndexId, value: Array[Byte], length: Byte, nodeId: NodeId): Unit = {
    db.put(KeyHandler.nodePropertyIndexKeyToBytes(indexId, value, Array[Byte](length), nodeId), Array.emptyByteArray)
  }

  // TODO
  private def writeIndexRowBatch(): Unit ={

  }

  private def deleteSingleIndexRow(indexId: IndexId, value: Array[Byte], length: Array[Byte], nodeId: NodeId): Unit = {
    db.delete(KeyHandler.nodePropertyIndexKeyToBytes(indexId, value, length, nodeId))
  }

  private def deleteIndexRows(indexId: IndexId): Unit = {
    db.deleteRange(KeyHandler.nodePropertyIndexKeyToBytes(indexId, zeroByteArray(8), Array.emptyByteArray, 0.toLong),
      KeyHandler.nodePropertyIndexKeyToBytes(indexId, oneByteArray(8), Array.emptyByteArray, -1.toLong))
  }

  private def updateIndexRow(indexId: IndexId, value: Array[Byte], length: Array[Byte], nodeId: NodeId, newValue: Array[Byte]): Unit = {
    deleteSingleIndexRow(indexId, value, length, nodeId)
    writeIndexRow(indexId, newValue, length, nodeId)
  }

  /**
   * Index
   */
  def createIndex(label: Int, props: Array[Int]): IndexId = {
    addIndexMeta(label, props)
  }

  def insertIndexRecord(indexId: IndexId, value: Array[Byte], nodeId: NodeId): Unit =  {
    writeIndexRow(indexId, value, value.length.toByte, nodeId )
  }

  def insertIndexRecord(indexId: IndexId, data: Iterator[(Array[Byte],Array[Byte], Long)]): Unit ={
    while (data.hasNext){
      val d = data.next()
      writeIndexRow(indexId, d._1, d._2, d._3)
    }
  }

  def updateIndexRecord(indexId: IndexId, value: Array[Byte], length: Array[Byte], nodeId: NodeId, newValue: Array[Byte]): Unit = {
    updateIndexRow(indexId, value, length, nodeId, newValue)
  }

  def deleteIndexRecord(indexId: IndexId, value: Array[Byte], length: Array[Byte], nodeId: NodeId): Unit ={
    deleteSingleIndexRow(indexId, value, length, nodeId)
  }

  def dropIndex(label: Int, props: Array[Int]): Unit = {
    deleteIndexRows(getIndexId(label, props))
    deleteIndexMeta(label, props)
  }

  def find(indexId: IndexId, value: Array[Byte], length: Array[Byte]): Iterator[NodeId] = {
    val iter = db.newIterator()
    val prefix = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, value, length)
    iter.seek(prefix)
    new Iterator[NodeId] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix) && iter.key().length-prefix.length==8

      override def next(): NodeId = {
        val key = iter.key()
        val id = ByteBuffer.wrap(key).getLong(key.length-8)
        iter.next()
        id
      }
    }
  }

  def find(indexId: IndexId, value: Array[Byte]): Iterator[NodeId] = find(indexId, value, Array[Byte](value.length.toByte))

  // TODO 有问题
  def findRange(indexId: IndexId, value: Array[Byte], length: Array[Byte]): Unit = {
  }

  private def zeroByteArray(len: Int): Array[Byte] = {
    new Array[Byte](len)
  }

  private def oneByteArray(len: Int): Array[Byte] = {
    val a = new Array[Byte](len)
    for (i <- a.indices)
      a(i) = -1
    a
  }
}
