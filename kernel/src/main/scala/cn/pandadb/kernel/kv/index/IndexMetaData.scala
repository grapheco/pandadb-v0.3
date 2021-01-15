package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.KeyConverter.{IndexId, KeyType, LabelId, PropertyId}
import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import org.rocksdb.RocksDB

import scala.collection.mutable

/**
 * @ClassName IndexMeta
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/23
 * @Version 0.1
 */
case class IndexMeta(indexId: IndexId, labelId: LabelId, isFullText: Boolean, props: PropertyId*)

class IndexMetaData(db: KeyValueDB) {

  val idMap: mutable.Map[IndexId, IndexMeta] = mutable.Map[IndexId, IndexMeta]()
  val labelMap: mutable.Map[LabelId, Set[IndexMeta]] = mutable.Map[IndexId, Set[IndexMeta]]()

  init()
  /**
   * Index MetaData
   *
   * ╔══════════════════════════════╦══════════════╗
   * ║              key             ║    value     ║
   * ╠═══════╦═══════╦══════════════╬══════════════╣
   * ║ label ║ props ║   fullText   ║   indexId    ║
   * ╚═══════╩═══════╩══════════════╩══════════════╝
   */
  def init(): Unit ={
    val iter = db.newIterator()
    iter.seekToFirst()
    while (iter.isValid) {
      val (l,p,f) = KeyConverter.getIndexMetaFromKey(iter.key())
      val id = ByteUtils.getInt(iter.value(),0)
      addMap(IndexMeta(id, l,f,p.sorted.toSeq:_*))
      iter.next()
    }
  }

  def addMap(meta: IndexMeta): Unit = {
    idMap.put(meta.indexId, meta)
    labelMap.put(meta.labelId, labelMap.getOrElse(meta.labelId, Set.empty) + meta)
  }

  def deleteMap(meta: IndexMeta): Unit = {
    idMap.remove(meta.indexId)
    labelMap.put(meta.labelId, labelMap.getOrElse(meta.labelId, Set.empty) - meta)
  }

  def addIndexMeta(label: Int, props: Array[Int], fulltext:Boolean = false, id: IndexId): Unit = {
    val key = KeyConverter.toIndexMetaKey(label, props, fulltext)
    addMap(IndexMeta(id, label,fulltext,props.sorted.toSeq:_*))
    db.put(key, ByteUtils.intToBytes(id))
  }

  def deleteIndexMeta(label: Int, props: Array[Int], fulltext:Boolean = false): Unit = {
    deleteMap(IndexMeta(getIndexId(label, props, fulltext).getOrElse(-1), label,fulltext,props.sorted.toSeq:_*))
    db.delete(KeyConverter.toIndexMetaKey(label, props, fulltext))
  }

  def getIndexId(label: Int, props: Array[Int], fulltext:Boolean = false): Option[IndexId] =
    getIndexId(label).filter(_.props == props.sorted.toSeq).map(_.indexId).headOption

  def getIndexMeta(indexId: IndexId): Option[IndexMeta] = idMap.get(indexId)

  def getIndexId(label: Int): Set[IndexMeta] = labelMap.getOrElse(label, Set.empty)

  def all(): Iterator[IndexMeta] = idMap.values.iterator
}
