package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler}
import org.rocksdb.{ReadOptions, RocksDB}

class RelationLabelIndex(db: RocksDB) {

  def add(labelId: Int, relId: Long): Unit ={
    val keyBytes = KeyHandler.relationLabelIndexKeyToBytes(labelId, relId)
    db.put(keyBytes, Array[Byte]())
  }

  def delete(labelId: Int, relId: Long): Unit = {
    val keyBytes = KeyHandler.relationLabelIndexKeyToBytes(labelId, relId)
    db.delete(keyBytes)
  }

  def getRelations(labelId: Int): Iterator[Long] = {
    val keyPrefix = KeyHandler.relationLabelIndexKeyPrefixToBytes(labelId)

    val readOptions = new ReadOptions()
    readOptions.setPrefixSameAsStart(true)
    readOptions.setTotalOrderSeek(true)
    val iter = db.newIterator(readOptions)
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

}
