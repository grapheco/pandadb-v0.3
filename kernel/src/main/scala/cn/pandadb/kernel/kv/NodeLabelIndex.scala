package cn.pandadb.kernel.kv

import org.rocksdb.{ReadOptions, RocksDB}

class NodeLabelIndex(db: RocksDB) {

  def add(labelId: Int, nodeId: Long): Unit ={
    val keyBytes = KeyHandler.nodeLabelIndexKeyToBytes(labelId, nodeId)
    db.put(keyBytes, null)

  }

  def remove(labelId: Int, nodeId: Long): Unit = {
    val keyBytes = KeyHandler.nodeLabelIndexKeyToBytes(labelId, nodeId)
    db.delete(keyBytes)
  }

  def getNodes(labelId: Int): Iterator[Long] = {
    val keyPrefix = KeyHandler.nodeLabelIndexKeyPrefixToBytes(labelId)

    val readOptions = new ReadOptions()
    readOptions.setPrefixSameAsStart(true)
    readOptions.setTotalOrderSeek(true)
    val iter = db.newIterator(readOptions)
    iter.seek(keyPrefix)

    new Iterator[Long] (){
      override def hasNext: Boolean = iter.isValid() && iter.key().startsWith(keyPrefix)

      override def next(): Long = {
        val nodeId: Long = ByteUtils.getLong(iter.key(), keyPrefix.length)
        iter.next()
        nodeId
      }
    }
  }

}
