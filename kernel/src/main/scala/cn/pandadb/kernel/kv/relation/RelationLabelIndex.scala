package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import org.rocksdb.{ReadOptions, RocksDB}

class RelationLabelIndex(db: RocksDB) {

  def set(labelId: Int, relId: Long): Unit ={
    val keyBytes = KeyConverter.toRelationTypeKey(labelId, relId)
    db.put(keyBytes, Array.emptyByteArray)
  }

  def delete(labelId: Int, relId: Long): Unit = {
    val keyBytes = KeyConverter.toRelationTypeKey(labelId, relId)
    db.delete(keyBytes)
  }

  def getRelations(labelId: Int): Iterator[Long] = {
    val keyPrefix = KeyConverter.toRelationTypeKey(labelId)
    val iter = db.newIterator()
    iter.seek(keyPrefix)

    new Iterator[Long] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(keyPrefix)

      override def next(): Long = {
        val relId: Long = ByteUtils.getLong(iter.key(), keyPrefix.length)
        iter.next()
        relId
      }
    }
  }

  def close(): Unit = db.close()

}
