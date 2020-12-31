package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.KeyHandler.KeyType
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler}
import org.rocksdb.RocksDB

/**
 * @ClassName IndexMeta
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/23
 * @Version 0.1
 */
class IndexMetaData(db: RocksDB) {

  type IndexId   = Int
  type NodeId    = Long
  val metaIdKey: Array[Byte] = Array[Byte](KeyType.NodePropertyIndexMeta.id.toByte)
  /**
   * Index MetaData
   *
   * ╔═══════════════╦══════════════╗
   * ║      key      ║    value     ║
   * ╠═══════╦═══════╬══════════════╣
   * ║ label ║ props ║   indexId    ║
   * ╚═══════╩═══════╩══════════════╝
   */
  def addIndexMeta(label: Int, props: Array[Int], fulltext:Boolean = false): IndexId = {
    val key = KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props)
    val id  = db.get(key)
    if (id == null || id.isEmpty){
      val new_id = getIncreasingId
      val id_byte = new Array[Byte](4)
      ByteUtils.setInt(id_byte, 0, new_id)
      db.put(key,id_byte)
      new_id
    } else {
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

  def deleteIndexMeta(label: Int, props: Array[Int], fulltext:Boolean = false): Unit = {
    db.delete(KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props))
  }

  def getIndexId(label: Int, props: Array[Int], fulltext:Boolean = false): Option[IndexId] = {
    val v = db.get(KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props))
    if (v == null || v.length < 4) None else Some(ByteUtils.getInt(v, 0))
  }

  def getIndexId(label: Int, prop: Int): Option[IndexId] = {
    getIndexId(label, Array[Int](prop))
  }

  def getIndexId(label: Int): Array[(Array[Int],IndexId)] = {
    val prefix = KeyHandler.nodePropertyIndexMetaKeyToBytes(label, Array.emptyIntArray)
    val iter = db.newIterator()
    iter.seek(prefix)
    new Iterator[(Array[Int],IndexId)] {
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
      override def next(): (Array[Int],IndexId) = {
        val props = (0 until (iter.key().length-5)/4).toArray.map(i => ByteUtils.getInt(iter.key(), 5+4*i))
        val id = ByteUtils.getInt(iter.value(), 0)
        iter.next()
        (props,id)
      }
    }.toArray
  }

  def all(): Iterator[IndexId] = {
    val iter = db.newIterator()
    iter.seekToFirst()
    new Iterator[IndexId] {
      override def hasNext: Boolean = iter.isValid

      override def next(): IndexId = {
        val id = ByteUtils.getInt(iter.value(), 0)
        iter.next()
        id
      }
    }
  }
}
