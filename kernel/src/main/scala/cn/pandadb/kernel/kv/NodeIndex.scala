package cn.pandadb.kernel.kv

import java.nio.ByteBuffer

import cn.pandadb.kernel.kv.KeyHandler.KeyType
import cn.pandadb.kernel.kv.NodeIndex.{IndexId, NodeId, metaIdKey}
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}
import java.lang.Float._
import java.lang.Double._

import scala.Byte.byte2int
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object NodeIndex{
  type IndexId   = Int
  type NodeId    = Long
  val metaIdKey: Array[Byte] = Array[Byte](KeyType.NodePropertyIndexMeta.id.toByte)

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
      val new_id = getIncreasingId
      val id_byte = new Array[Byte](4)
      ByteUtils.setInt(id_byte, 0, new_id)
      db.put(key,id_byte)
      new_id
    } else {
      // exist
      ByteUtils.getInt(id, 0)
    }
  }

  def getIncreasingId: IndexId ={
    val increasingId = db.get(metaIdKey)
    var id:Int = 0
    if (increasingId != null && increasingId.nonEmpty){
      id = ByteUtils.getInt(increasingId, 0)
    }
    val id_bytes = ByteUtils.intToBytes(id+1)
    db.put(metaIdKey, id_bytes)
    id
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
  private def writeIndexRowBatch(indexId: IndexId, data: Iterator[(Array[Byte],Array[Byte], Long)]): Unit ={
    val writeOpt = new WriteOptions()
    val batch = new WriteBatch()
    var i = 0
    while (data.hasNext){
      val d = data.next()
      batch.put(KeyHandler.nodePropertyIndexKeyToBytes(indexId, d._1, d._2, d._3), Array.emptyByteArray)
      if (i % 10000 == 0){
        db.write(writeOpt, batch)
        batch.clear()
      }
      i += 1
    }
    db.write(writeOpt, batch)
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

  def insertIndexRecordBatch(indexId: IndexId, data: Iterator[(Array[Byte],Array[Byte], Long)]): Unit ={
    writeIndexRowBatch(indexId: IndexId, data: Iterator[(Array[Byte],Array[Byte], Long)])
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

  def find(indexId: IndexId, value: Array[Byte], length: Array[Byte], strictLength: Boolean = true): Iterator[NodeId] = {
    val iter = db.newIterator()
    val prefix = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, value, length)
    iter.seek(prefix)
    new Iterator[NodeId] (){
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(prefix) && (!strictLength || iter.key().length-prefix.length==8)

      override def next(): NodeId = {
        val key = iter.key()
        val id = ByteBuffer.wrap(key).getLong(key.length-8)
        iter.next()
        id
      }
    }
  }

  def find(indexId: IndexId, value: Array[Byte]): Iterator[NodeId] = find(indexId, value, Array[Byte](value.length.toByte))

  def findStringStartWith(indexId: IndexId, stringByteArray: Array[Byte]): Iterator[NodeId] = find(indexId, stringByteArray, Array.emptyByteArray, strictLength = false)

  def findIntRange(indexId: IndexId, startValue: Int = Int.MinValue, endValue: Int = Int.MaxValue): Iterator[NodeId] = {
    val iter = db.newIterator()
    val startPrefix = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, NumberEncoder.encode(startValue), zeroByteArray(1))
    val endPrefix   = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, NumberEncoder.encode(endValue), oneByteArray(1))
    iter.seekForPrev(endPrefix)
    val endKey = iter.key()
    iter.seek(startPrefix)
    new Iterator[NodeId] (){
      var end = false
      override def hasNext: Boolean = iter.isValid && !end
      override def next(): NodeId = {
        val key = iter.key()
        if(key.startsWith(endKey)) end = true
        val id = ByteBuffer.wrap(key).getLong(key.length-8)
        iter.next()
        id
      }
    }
  }

  def findDoubleRange(indexId: IndexId, startValue: Double, endValue: Double): Iterator[NodeId] = {
    val iter = db.newIterator()

    val startPrefix = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, NumberEncoder.encode(startValue), zeroByteArray(1))
    val endPrefix   = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, NumberEncoder.encode(endValue), oneByteArray(1))

    val minPositive  = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, Array[Byte](0), Array.emptyByteArray)

    println("min",minPositive.mkString("Array(", ", ", ")"))
    val across = startValue < 0 && endValue >= 0
    var direct = startValue >= 0
    val end_direct = endValue >= 0

    iter.seekForPrev(endPrefix)
    if (!end_direct){
      iter.next()
    }
    val end_key = iter.key()
    println(end_key.foreach(print(_)))
    iter.seek(startPrefix)
    new Iterator[NodeId] (){
      var end = false
      override def hasNext: Boolean = iter.isValid && !end
      override def next(): NodeId = {
        val key = iter.key()
        val symbol = if(direct) 0.toByte else Byte.MinValue
        if (!key.startsWith(KeyHandler.nodePropertyIndexPrefixToBytes(indexId, Array[Byte](symbol), Array.emptyByteArray))){
          // across
          if(direct) end = true
          else{
            iter.seek(minPositive)
            direct = true
          }
        }
        if(key.startsWith(end_key)) end = true
        val id = ByteBuffer.wrap(key).getLong(key.length-8)
        if (direct)
          iter.next()
        else
          iter.prev()
        id
      }
    }
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

object NumberEncoder{


  def encode(number: Any): Array[Byte] = {
    number match {
      case number:Byte => ByteUtils.byteToBytes((number^Byte.MinValue).toByte)
      case number: Short => ByteUtils.shortToBytes((number^Short.MinValue).toShort)
      case number: Int => ByteUtils.intToBytes(number^Int.MinValue)
      case number: Float => ByteUtils.floatToBytes(number)
      case number: Long => ByteUtils.longToBytes(number^Long.MinValue)
      case number: Double => ByteUtils.doubleToBytes(number)
      case v1: Any => throw new Exception(s"this value type: ${v1.getClass} is not number")
    }
  }

}
