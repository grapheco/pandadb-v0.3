package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.KeyHandler
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.rocksdb.{ReadOptions, RocksDB}

class NewNodeStore(db: RocksDB) {
  // [type,labelId,nodeId]->[labels + properties]

  val NONE_LABEL_ID = 0

  def set(nodeId: Long, labelIds: Array[Int], properties: Map[Int, Any]): Unit = {
    val valueBytes = BaseSerializer.intArrayMap2Bytes(labelIds, properties)
    labelIds.foreach(labelId => {
      val keyBytes = KeyHandler.newNodeKeyToBytes(labelId, nodeId)
      db.put(keyBytes, valueBytes)
    })
    val keyBytes = KeyHandler.newNodeKeyToBytes(NONE_LABEL_ID, nodeId)
    db.put(keyBytes, valueBytes)
  }

  def set(node: StoredNodeWithProperty): Unit = {
    set(node.id, node.labelIds, node.properties)
  }

  def get(nodeId: Long): StoredNodeWithProperty = {
    val keyBytes = KeyHandler.newNodeKeyToBytes(NONE_LABEL_ID, nodeId)
    val valueBytes = db.get(keyBytes)
    val (labelIds, properties) = BaseSerializer.bytes2IntArrayMap(valueBytes)
    new StoredNodeWithProperty(nodeId, labelIds, properties)
  }

  def all() : Iterator[StoredNodeWithProperty] = {
    val keyPrefix = KeyHandler.newNodeKeyPrefixToBytes(NONE_LABEL_ID)
    val readOptions = new ReadOptions()
    readOptions.setPrefixSameAsStart(true)
    readOptions.setTotalOrderSeek(true)
    val iter = db.newIterator(readOptions)
    iter.seek(keyPrefix)

    new Iterator[StoredNodeWithProperty] (){
      override def hasNext: Boolean = iter.isValid() && iter.key().startsWith(keyPrefix)
      override def next(): StoredNodeWithProperty = {
        val nodeId = KeyHandler.parseNewNodeKeyFromBytes(iter.key())._2
        val (labelIds, properties) = BaseSerializer.bytes2IntArrayMap(iter.value())
        iter.next()
        new StoredNodeWithProperty(nodeId, labelIds, properties)
      }
    }

  }

  def delete(nodeId:Long, labelId: Int=NONE_LABEL_ID): Unit = {
    db.delete(KeyHandler.newNodeKeyToBytes(labelId, nodeId))
  }

  def delete(nodeId:Long, labelIds: Iterable[Int]): Unit = {
    labelIds.foreach(labelId => {
      delete(nodeId, labelId)
    })
    delete(nodeId)
  }

  def delete(node: StoredNodeWithProperty): Unit = {
    delete(node.id, node.labelIds.toIterable)
  }

}
