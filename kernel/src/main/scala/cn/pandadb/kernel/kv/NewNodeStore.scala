package cn.pandadb.kernel.kv

import cn.pandadb.kernel.store.StoredNodeWithProperty
import org.rocksdb.RocksDB

class NewNodeStore(db: RocksDB) {
  // [type,labelId,nodeId]->[labels + properties]

  val NONE_LABEL_ID = 0

  def add(node: StoredNodeWithProperty): Unit = {
    val valueBytes = Array[Byte]() // todo: update with ByteUtils.objectToBytes()
    if (node.labelIds != null && node.labelIds.size > 0) {
      node.labelIds.foreach(labelId => {
        val keyBytes = KeyHandler.newNodeKeyToBytes(labelId, node.id)
        db.put(keyBytes, valueBytes)
      })
      // if [NONE_LABEL_ID, NodeId]->[node value] exists, delete it
      delete(node.id)
    }
    else {
      val keyBytes = KeyHandler.newNodeKeyToBytes(NONE_LABEL_ID, node.id)
      db.put(keyBytes, valueBytes)
    }
  }

  def add(nodeId: Long, labelId: Int = NONE_LABEL_ID, properties: Map[Any, Any] = Map() ) : Unit = {
    val valueBytes = Array[Byte]() // todo: update with ByteUtils.objectToBytes()
    val keyBytes = KeyHandler.newNodeKeyToBytes(labelId, nodeId)
    db.put(keyBytes, valueBytes)
  }

  def delete(nodeId:Long, labelId: Int=NONE_LABEL_ID): Unit = {
    db.delete( KeyHandler.newNodeKeyToBytes(labelId, nodeId))
  }

  def delete(nodeId:Long, labelIds: Iterable[Int]): Unit = {
    if (labelIds == null || labelIds.size == 0) {
      delete(nodeId)
    }
    else {
      labelIds.foreach(labelId => {
        delete(nodeId, labelId)
      })
    }
  }

  def delete(node: StoredNodeWithProperty): Unit = {
    delete(node.id, node.labelIds.toIterable)
  }



}
