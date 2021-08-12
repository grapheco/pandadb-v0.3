package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.kv.KeyConverter.{LabelId, NodeId}
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.rocksdb.{Transaction, TransactionDB, WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:12 上午 2021/8/9
 * @Modified By:
 */
class TransactionNodeStore(db: TransactionDB) {
  // [labelId,nodeId]->[Node]
  val NONE_LABEL_ID: Int = 0

  def set(nodeId: NodeId, labelIds: Array[LabelId], value: Array[Byte], tx: Transaction): Unit = {
    if (labelIds.nonEmpty)
      labelIds.foreach(labelId => tx.put(KeyConverter.toNodeKey(labelId, nodeId), value))
    else
      tx.put(KeyConverter.toNodeKey(NONE_LABEL_ID, nodeId), value)
  }

  def set(labelId: LabelId, node: StoredNodeWithProperty, tx: Transaction): Unit =
    tx.put(KeyConverter.toNodeKey(labelId, node.id), NodeSerializer.serialize(node))

  def set(node: StoredNodeWithProperty, tx: Transaction): Unit =
    set(node.id, node.labelIds, NodeSerializer.serialize(node), tx)

  def get(nodeId: NodeId, labelId: LabelId): Option[StoredNodeWithProperty] = {
    val value = db.get(KeyConverter.toNodeKey(labelId, nodeId))
    if(value != null) Some(NodeSerializer.deserializeNodeValue(value))
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
    val prefix = KeyConverter.toNodeKey(labelId)
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
    val prefix = KeyConverter.toNodeKey(labelId)
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

  def getNodesByLabelWithoutDeserialize(labelId: LabelId): Iterator[NodeId] = {
    val iter = db.newIterator()
    val prefix = KeyConverter.toNodeKey(labelId)
    iter.seek(prefix)

    new Iterator[NodeId] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
      override def next(): NodeId = {
        val id = ByteUtils.getLong(iter.key(), prefix.length)
        iter.value().length
        iter.next()
        id
      }
    }
  }

  def deleteByLabel(labelId: LabelId, tx: Transaction): Unit = {
    this.synchronized{
      val batch = new WriteBatch()
      batch.deleteRange(KeyConverter.toNodeKey(labelId, 0.toLong),
        KeyConverter.toNodeKey(labelId, -1.toLong))
      tx.rebuildFromWriteBatch(batch)
    }
  }


  def delete(nodeId: NodeId, labelId: LabelId, tx: Transaction): Unit = tx.delete(KeyConverter.toNodeKey(labelId, nodeId))


  def delete(nodeId:Long, labelIds: Array[LabelId], tx: Transaction): Unit = labelIds.foreach(delete(nodeId, _, tx))

  def delete(node: StoredNodeWithProperty, tx: Transaction): Unit = delete(node.id, node.labelIds, tx)
}
