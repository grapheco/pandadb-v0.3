package cn.pandadb.kernel.kv

import org.rocksdb.{ReadOptions, RocksDB}

class NodeLabelIndex2(db: RocksDB) {
  // [type,labelId,nodeId]->[properties]


  def set(labelId: Int, nodeId: Long, properties: Array[Byte]): Unit ={
    val keyBytes = KeyHandler.nodeLabelIndexKeyToBytes(labelId, nodeId)
    db.put(keyBytes, properties)
  }

  def delete(labelId: Int, nodeId: Long): Unit = {
    val keyBytes = KeyHandler.nodeLabelIndexKeyToBytes(labelId, nodeId)
    db.delete(keyBytes)
  }

  def get(labelId: Int, nodeId: Long): Array[Byte] = {
    val keyBytes = KeyHandler.nodeLabelIndexKeyToBytes(labelId, nodeId)
    db.get(keyBytes)
  }

  def seek(labelId: Int): Iterator[(Long, Array[Byte])] = {
    val keyPrefix = KeyHandler.nodeLabelIndexKeyPrefixToBytes(labelId)

    val readOptions = new ReadOptions()
    readOptions.setPrefixSameAsStart(true)
    readOptions.setTotalOrderSeek(true)
    val iter = db.newIterator(readOptions)
    iter.seek(keyPrefix)

    new Iterator[(Long, Array[Byte])] (){
      override def hasNext: Boolean = iter.isValid() && iter.key().startsWith(keyPrefix)

      override def next(): (Long, Array[Byte]) = {
        val ret = (ByteUtils.getLong(iter.key(), keyPrefix.length), iter.value())
        iter.next()
        ret
      }
    }
  }

}
