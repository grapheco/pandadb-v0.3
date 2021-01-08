package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler}
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.serializer.{BaseSerializer, NodeSerializer}
import org.rocksdb.{ReadOptions, RocksDB}

class NodeStore(db: RocksDB) {
  // [type,labelId,nodeId]->[Node]
  type NodeId = Long
  type LabelId = Int

  def set(nodeId: NodeId, labelIds: Array[LabelId], value: Array[Byte]): Unit =
    labelIds.foreach(labelId =>
      db.put(KeyHandler.nodeKeyToBytes(labelId, nodeId), value))

  def set(labelId: LabelId, node: StoredNodeWithProperty): Unit =
    db.put(KeyHandler.nodeKeyToBytes(labelId, node.id), NodeSerializer.serialize(node))

  def set(node: StoredNodeWithProperty): Unit =
    set(node.id, node.labelIds, NodeSerializer.serialize(node))

  def get(nodeId: NodeId, labelId: LabelId): Option[StoredNodeWithProperty] = {
    val value = db.get(KeyHandler.nodeKeyToBytes(labelId, nodeId))
    if(value.nonEmpty) Some(NodeSerializer.deserializeNodeValue(value))
    else None
  }

  def all() : Iterator[StoredNodeWithProperty] = {
    val iter = db.newIterator()
    iter.seekToFirst()

    new Iterator[StoredNodeWithProperty] (){
      override def hasNext: Boolean = iter.isValid
      override def next(): StoredNodeWithProperty = {
        val node = NodeSerializer.deserializeNodeValue(iter.value())
        val label = ByteUtils.getInt(iter.key(), 0)
        iter.next()
        if (node.labelIds.length > 0 && node.labelIds(0) != label) null
        else node
      }
    }.filter(_!=null)
  }

  def getNodesByLabel(labelId: LabelId): Iterator[StoredNodeWithProperty] = {
    val iter = db.newIterator()
    val prefix = KeyHandler.nodePrefix(labelId)
    iter.seek(prefix)

    new Iterator[StoredNodeWithProperty] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
      override def next(): StoredNodeWithProperty = {
        val node = NodeSerializer.deserializeNodeValue(iter.value())
        iter.next()
        node
      }
    }
  }

  def getNodeIdsByLabel(labelId: LabelId): Iterator[NodeId] = {
    val iter = db.newIterator()
    val prefix = KeyHandler.nodePrefix(labelId)
    iter.seek(prefix)

    new Iterator[NodeId] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
      override def next(): NodeId = {
        val id = ByteUtils.getLong(iter.key(), prefix.length)
        iter.next()
        id
      }
    }
  }

  def deleteByLabel(labelId: LabelId): Unit =
    db.deleteRange(KeyHandler.nodeKeyToBytes(labelId, 0.toLong),
      KeyHandler.nodeKeyToBytes(labelId, -1.toLong))


  def delete(nodeId: NodeId, labelId: LabelId): Unit = db.delete(KeyHandler.nodeKeyToBytes(labelId, nodeId))


  def delete(nodeId:Long, labelIds: Array[LabelId]): Unit = labelIds.foreach(delete(nodeId, _))

  def delete(node: StoredNodeWithProperty): Unit = delete(node.id, node.labelIds)

}
