package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.KeyConverter.KeyType
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
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
  /**
   * Index MetaData
   *
   * ╔══════════════════════════════╦══════════════╗
   * ║              key             ║    value     ║
   * ╠═══════╦═══════╦══════════════╬══════════════╣
   * ║ label ║ props ║   fullText   ║   indexId    ║
   * ╚═══════╩═══════╩══════════════╩══════════════╝
   */
  def addIndexMeta(label: Int, props: Array[Int], fulltext:Boolean = false, id: IndexId): Unit = {
    val key = KeyConverter.toIndexMetaKey(label, props, fulltext)
    db.put(key, ByteUtils.intToBytes(id))
  }

  def deleteIndexMeta(label: Int, props: Array[Int], fulltext:Boolean = false): Unit = {
    db.delete(KeyConverter.toIndexMetaKey(label, props, fulltext))
  }

  def getIndexId(label: Int, props: Array[Int], fulltext:Boolean = false): Option[IndexId] = {
    val v = db.get(KeyConverter.toIndexMetaKey(label, props, fulltext))
    if (v == null || v.length < 4) None else Some(ByteUtils.getInt(v, 0))
  }

  def getIndexId(label: Int): Array[(Array[Int],IndexId, Boolean)] = {
    val prefix = KeyConverter.toIndexMetaKey(label, Array.emptyIntArray, false)
    val iter = db.newIterator()
    iter.seek(prefix)
    new Iterator[(Array[Int],IndexId,Boolean)] {
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
      override def next(): (Array[Int],IndexId,Boolean) = {
        val props = (0 until (iter.key().length-5)/4).toArray.map(i => ByteUtils.getInt(iter.key(), 4+4*i))
        val id = ByteUtils.getInt(iter.value(), 0)
        val isFullText = ByteUtils.getBoolean(iter.key(), iter.key().length - 1)
        iter.next()
        (props, id, isFullText)
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
