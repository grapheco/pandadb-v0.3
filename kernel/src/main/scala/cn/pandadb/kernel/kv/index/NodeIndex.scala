package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.index.NodeIndex.{IndexId, NodeId, metaIdKey}
import cn.pandadb.kernel.kv.KeyHandler.KeyType
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler}
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}


object NodeIndex{
  type IndexId   = Int
  type NodeId    = Long
  val metaIdKey: Array[Byte] = Array[Byte](KeyType.NodePropertyIndexMeta.id.toByte)

}

class NodeIndex(db: RocksDB){

  /**
   * Index MetaData
   *
   * ╔═══════════════╦══════════════╗
   * ║      key      ║    value     ║
   * ╠═══════╦═══════╬══════════════╣
   * ║ label ║ props ║   indexId    ║
   * ╚═══════╩═══════╩══════════════╝
   */
  private def addIndexMeta(label: Int, props: Array[Int]): IndexId = {
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

  private def getIncreasingId: IndexId ={
    val increasingId = db.get(metaIdKey)
    var id:Int = 0
    if (increasingId != null && increasingId.nonEmpty){
      id = ByteUtils.getInt(increasingId, 0)
    }
    val id_bytes = ByteUtils.intToBytes(id+1)
    db.put(metaIdKey, id_bytes)
    id
  }

  private def deleteIndexMeta(label: Int, props: Array[Int]): Unit = {
    db.delete(KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props))
  }

  def getIndexId(label: Int, props: Array[Int]): IndexId = {
    val v = db.get(KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props))
    if (v == null || v.length < 4) -1 else ByteUtils.getInt(v, 0)
  }

  def getIndexId(label: Int, prop: Int): IndexId = {
    getIndexId(label, Array[Int](prop))
  }

  /**
   * Single Column Index:
   * ╔══════════════════════════════════════════╗
   * ║                   key                    ║
   * ╠═════════╦══════════╦══════════╦══════════╣
   * ║ indexId ║ typeCode ║  value   ║  nodeId  ║
   * ╚═════════╩══════════╩══════════╩══════════╝
   */

  private def writeIndexRow(indexId: IndexId,typeCode:Byte, value: Array[Byte], nodeId: NodeId): Unit = {
    db.put(KeyHandler.nodePropertyIndexKeyToBytes(indexId, typeCode, value, nodeId), Array.emptyByteArray)
  }

  private def writeIndexRowBatch(indexId: IndexId, data: Iterator[(Any, Long)]): Unit ={
    val writeOpt = new WriteOptions()
    val batch = new WriteBatch()
    var i = 0
    while (data.hasNext){
      val d = data.next()
      batch.put(
        KeyHandler.nodePropertyIndexKeyToBytes(indexId, IndexEncoder.typeCode(d._1), IndexEncoder.encode(d._1), d._2),
        Array.emptyByteArray)
      if (i % 100000 == 0){
        db.write(writeOpt, batch)
        batch.clear()
      }
      i += 1
    }
    db.write(writeOpt, batch)
  }

  private def deleteSingleIndexRow(indexId: IndexId, typeCode:Byte, value: Array[Byte], nodeId: NodeId): Unit = {
    db.delete(KeyHandler.nodePropertyIndexKeyToBytes(indexId, typeCode, value, nodeId))
  }

  private def deleteIndexRows(indexId: IndexId): Unit = {
    db.deleteRange(KeyHandler.nodePropertyIndexKeyToBytes(indexId, 0, Array.emptyByteArray, 0.toLong),
      KeyHandler.nodePropertyIndexKeyToBytes(indexId, Byte.MaxValue, Array.emptyByteArray, -1.toLong))
  }

  private def updateIndexRow(indexId: IndexId, typeCode:Byte, value: Array[Byte],
                             nodeId: NodeId, newTypeCode:Byte, newValue: Array[Byte] ): Unit = {
    deleteSingleIndexRow(indexId, typeCode, value,  nodeId)
    writeIndexRow(indexId, newTypeCode, newValue, nodeId)
  }

  /**
   * APIs
   */
  def createIndex(label: Int, props: Array[Int]): IndexId = {
    addIndexMeta(label, props)
  }

  def insertIndexRecord(indexId: IndexId, data: Any, nodeId: NodeId): Unit = {
    writeIndexRow(indexId, IndexEncoder.typeCode(data), IndexEncoder.encode(data), nodeId)
  }

  def insertIndexRecordBatch(indexId: IndexId, data: Iterator[(Any, Long)]): Unit =
    writeIndexRowBatch(indexId, data)

  def updateIndexRecord(indexId: IndexId, value: Any, nodeId: NodeId, newValue: Any): Unit = {
    updateIndexRow(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value),
      nodeId, IndexEncoder.typeCode(newValue), IndexEncoder.encode(newValue))
  }

  def deleteIndexRecord(indexId: IndexId, value: Any, nodeId: NodeId): Unit ={
    deleteSingleIndexRow(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value), nodeId)
  }

  def dropIndex(label: Int, props: Array[Int]): Unit = {
    deleteIndexRows(getIndexId(label, props))
    deleteIndexMeta(label, props)
  }

  def findByPrefix(prefix: Array[Byte]): Iterator[NodeId] = {
    val iter = db.newIterator()
    iter.seek(prefix)
    new Iterator[NodeId] (){
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(prefix)

      override def next(): NodeId = {
        val key = iter.key()
        val id = ByteUtils.getLong(key, key.length-8)
        iter.next()
        id
      }
    }
  }

  def find(indexId: IndexId, value: Any): Iterator[NodeId] =
    findByPrefix(KeyHandler.nodePropertyIndexPrefixToBytes(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value)))

  private def findRange(indexId:IndexId, valueType: Byte, startValue: Array[Byte] , endValue: Array[Byte]): Iterator[NodeId] = {
    val typePrefix  = KeyHandler.nodePropertyIndexTypePrefix(indexId, valueType)
    val startPrefix = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, valueType, startValue)
    val endPrefix   = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, valueType, endValue)
    val iter = db.newIterator()
    iter.seekForPrev(endPrefix)
    val endKey = iter.key()
    iter.seek(startPrefix)
    new Iterator[NodeId] (){
      var end = false
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(typePrefix) && !end
      override def next(): NodeId = {
        val key = iter.key()
        end = key.startsWith(endKey)
        val id = ByteUtils.getLong(key, key.length-8)
        iter.next()
        id
      }
    }
  }


  def findStringStartWith(indexId: IndexId, string: String):  Iterator[NodeId] =
    findByPrefix(
      KeyHandler.nodePropertyIndexPrefixToBytes(
        indexId,
        IndexEncoder.STRING_CODE,
        IndexEncoder.encode(string).take(string.getBytes().length / 8 + string.getBytes().length)))


  def findIntRange(indexId: IndexId, startValue: Int = Int.MinValue, endValue: Int = Int.MaxValue): Iterator[NodeId] =
    findRange(indexId, IndexEncoder.INT_CODE, IndexEncoder.encode(startValue), IndexEncoder.encode(endValue))

  def findFloatRange(indexId: IndexId, startValue: Float, endValue: Float): Iterator[NodeId] =
    findRange(indexId, IndexEncoder.FLOAT_CODE, IndexEncoder.encode(startValue), IndexEncoder.encode(endValue))




}
