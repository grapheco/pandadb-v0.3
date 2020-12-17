package cn.pandadb.kernel.kv

import java.nio.ByteBuffer

import cn.pandadb.kernel.kv.KeyHandler.{KeyType}
import cn.pandadb.kernel.kv.NodeIndex.{IndexId, NodeId, metaIdKey}
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
        KeyHandler.nodePropertyIndexKeyToBytes(indexId, IndexValue.typeCode(d._1), IndexValue.encode(d._1), d._2),
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
   * Index
   */
  def createIndex(label: Int, props: Array[Int]): IndexId = {
    addIndexMeta(label, props)
  }

  def insertIndexRecord(indexId: IndexId, data: Any, nodeId: NodeId): Unit = {
    writeIndexRow(indexId, IndexValue.typeCode(data), IndexValue.encode(data), nodeId)
  }

  def insertIndexRecordBatch(indexId: IndexId, data: Iterator[(Any, Long)]): Unit =
    writeIndexRowBatch(indexId, data)

  def updateIndexRecord(indexId: IndexId, value: Any, nodeId: NodeId, newValue: Any): Unit = {
    updateIndexRow(indexId, IndexValue.typeCode(value), IndexValue.encode(value),
      nodeId, IndexValue.typeCode(newValue), IndexValue.encode(newValue))
  }

  def deleteIndexRecord(indexId: IndexId, value: Any, nodeId: NodeId): Unit ={
    deleteSingleIndexRow(indexId, IndexValue.typeCode(value), IndexValue.encode(value), nodeId)
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
        val id = ByteBuffer.wrap(key).getLong(key.length-8)
        iter.next()
        id
      }
    }
  }

  def find(indexId: IndexId, value: Any): Iterator[NodeId] =
    findByPrefix(KeyHandler.nodePropertyIndexPrefixToBytes(indexId, IndexValue.typeCode(value), IndexValue.encode(value)))

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
        val id = ByteBuffer.wrap(key).getLong(key.length-8)
        iter.next()
        id
      }
    }
  }

  def findStringStartWith(indexId: IndexId, string: Array[Byte]): Iterator[NodeId] = find(indexId, string)

  def findStringStartWith(indexId: IndexId, string: String):  Iterator[NodeId] =
    findByPrefix(
      KeyHandler.nodePropertyIndexPrefixToBytes(
        indexId,
        IndexValue.STRING_CODE,
        IndexValue.encode(string).take(string.getBytes().length / 8 + string.getBytes().length)))


  def findIntRange(indexId: IndexId, startValue: Int = Int.MinValue, endValue: Int = Int.MaxValue): Iterator[NodeId] =
    findRange(indexId, IndexValue.INT_CODE, IndexValue.encode(startValue), IndexValue.encode(endValue))

  def findFloatRange(indexId: IndexId, startValue: Float, endValue: Float): Iterator[NodeId] =
    findRange(indexId, IndexValue.FLOAT_CODE, IndexValue.encode(startValue), IndexValue.encode(endValue))

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

object IndexValue{

  val NULL: Byte      = -1
  val FALSE_CODE:Byte = 0
  val TRUE_CODE:Byte  = 1
  val INT_CODE :Byte  = 2
  val FLOAT_CODE:Byte = 3
  val LONG_CODE:Byte  = 4
  val STRING_CODE:Byte= 5

  /**
   * support dataType : boolean, byte, short, int, float, long, double, string
   * ╔═══════════════╦══════════════╦══════════════╗
   * ║    dataType   ║  storageType ║    typeCode  ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Null     ║    Empty     ║      -1      ║  ==> TODO
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      False    ║    Empty     ║      0       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      True     ║    Empty     ║      1       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Byte     ║    *Int*     ║      2       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Short    ║    *Int*     ║      2       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Int      ║     Int      ║      2       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Float    ║    Float     ║      3       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║     Double    ║   *Float*    ║      3       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Long     ║     Long     ║      4       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║     String    ║    String    ║      5       ║
   * ╚═══════════════╩══════════════╩══════════════╝
   *
   * @param data the data to encode
   * @return the bytes array after encode
   */
  def encode(data: Any): Array[Byte] = {
    data match {
      case data: Boolean => Array.emptyByteArray
      case data: Byte => intEncode(data.toInt)
      case data: Short => intEncode(data.toInt)
      case data: Int => intEncode(data)
      case data: Float => floatEncode(data)
      case data: Long => longEncode(data)
      case data: Double => floatEncode(data.toFloat)
      case data: String =>stringEncode(data)
      case data: Any => throw new Exception(s"this value type: ${data.getClass} is not supported")
    }
  }

  def typeCode(data: Any): Byte = {
    data match {
      case data: Boolean if data => TRUE_CODE
      case data: Boolean if !data => FALSE_CODE
      case data: Byte => INT_CODE
      case data: Short => INT_CODE
      case data: Int => INT_CODE
      case data: Float => FLOAT_CODE
      case data: Long => LONG_CODE
      case data: Double => FLOAT_CODE
      case data: String =>STRING_CODE
      case data: Any => throw new Exception(s"this value type: ${data.getClass} is not supported")
    }
  }


  private def intEncode(int: Int): Array[Byte] = {
    ByteUtils.intToBytes(int^Int.MinValue)
  }

  /**
   * if float greater than or equal 0, the highest bit set to 1
   * else NOT each bit
   */
  private def floatEncode(float: Float): Array[Byte] = {
    val buf = ByteUtils.floatToBytes(float)
    if (float >= 0) {
      // 0xxxxxxx => 1xxxxxxx
      ByteUtils.setInt(buf, 0, ByteUtils.getInt(buf, 0)^Int.MinValue)
    } else {
      // ~
      ByteUtils.setInt(buf, 0, ~ ByteUtils.getInt(buf, 0))
    }
    buf
  }

  private def longEncode(long: Long): Array[Byte] = {
    ByteUtils.longToBytes(long^Long.MinValue)
  }

  private def stringEncode_old(string: String): Array[Byte] = {
    ByteUtils.stringToBytes(string)
  }


  /**
   * The string is divided into groups according to 8 bytes.
   * The last group is less than 8 bytes, and several zeros bytes are supplemented.
   * Add a byte to the end of each group. The value of the byte is 255 minus the number of 0 bytes filled by the group.
   * eg:
   * []                   =>  [0,0,0,0,0,0,0,0,247]
   * [1,2,3]              =>  [1,2,3,0,0,0,0,0,250]
   * [1,2,3,0]            =>  [1,2,3,0,0,0,0,0,251]
   * [1,2,3,4,5,6,7,8]    =>  [1,2,3,4,5,6,7,8,255,0,0,0,0,0,0,0,0,247]
   * [1,2,3,4,5,6,7,8,9]  =>  [1,2,3,4,5,6,7,8,255,9,0,0,0,0,0,0,0,248]
   */
  private def stringEncode(string: String): Array[Byte] = {
    val buf = ByteUtils.stringToBytes(string)
    val group = buf.length/8 +1
    val res = new Array[Byte](group*9)
    // set value Bytes
    for (i <- buf.indices){
      res(i+i/8) = buf(i)
    }
    // set length Bytes
    for (i <- 1 to group-1) {
      res(9*i -1) = 255.toByte
    }
    // set last Bytes
    res(res.length-1) = (247 + buf.length%8).toByte
    res
  }


}
