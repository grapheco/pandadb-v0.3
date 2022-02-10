package org.grapheco.pandadb.kernel.distribute.node

import org.grapheco.pandadb.kernel.distribute.DistributedKeyConverter.{LabelId, NodeId}
import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.grapheco.pandadb.kernel.store.StoredNodeWithProperty
import org.grapheco.pandadb.kernel.util.serializer.NodeSerializer
import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-16 14:28
 */
class NodeStore(db: DistributedKVAPI) {
  implicit def ByteString2ArrayByte(data: ByteString) = data.toByteArray

  val NONE_LABEL_ID: Int = 0
  val BATCH_SIZE = 10000

  def set(nodeId: NodeId, labelIds: Array[LabelId], value: Array[Byte]): Unit = {
    if (labelIds.nonEmpty)
      labelIds.foreach(labelId => db.put(DistributedKeyConverter.toNodeKey(labelId, nodeId), value))
    else
      db.put(DistributedKeyConverter.toNodeKey(NONE_LABEL_ID, nodeId), value)
  }

  def set(labelId: LabelId, node: StoredNodeWithProperty): Unit =
    db.put(DistributedKeyConverter.toNodeKey(labelId, node.id), NodeSerializer.serialize(node))

  def set(node: StoredNodeWithProperty): Unit =
    set(node.id, node.labelIds, NodeSerializer.serialize(node))

  def get(nodeId: NodeId, labelId: LabelId): Option[StoredNodeWithProperty] = {
    val value = db.get(DistributedKeyConverter.toNodeKey(labelId, nodeId))
    if(value.nonEmpty) Some(NodeSerializer.deserializeNodeValue(value))
    else None
  }

  def all() : Iterator[StoredNodeWithProperty] = {
    val iter = db.scanPrefix(Array(DistributedKeyConverter.nodeKeyPrefix), BATCH_SIZE, false)

    new Iterator[StoredNodeWithProperty]{
      override def hasNext: Boolean = iter.hasNext

      override def next(): StoredNodeWithProperty = {
        val data = iter.next()
        val node = NodeSerializer.deserializeNodeValue(data.getValue)
        val label = ByteUtils.getInt(data.getKey, DistributedKeyConverter.KEY_PREFIX_SIZE)
        if (node.labelIds.length > 0 && node.labelIds.head != label) null
        else node
      }
    }.filter(_ != null)
  }

  def getNodesByLabel(labelId: LabelId): Iterator[StoredNodeWithProperty] = {
    val prefix = DistributedKeyConverter.toNodeKey(labelId)
    val iter = db.scanPrefix(prefix, BATCH_SIZE, false)

    new Iterator[StoredNodeWithProperty] (){
      override def hasNext: Boolean = iter.hasNext
      override def next(): StoredNodeWithProperty = {
        NodeSerializer.deserializeNodeValue(iter.next().getValue)
      }
    }
  }

  def getNodesByIds(labelId: LabelId, ids: Seq[Long]): Iterator[StoredNodeWithProperty] = {
    val keys = ids.map(id => DistributedKeyConverter.toNodeKey(labelId, id))
    val nodes = db.batchGetValue(keys)
    nodes.map(NodeSerializer.deserializeNodeValue(_))
  }

  def getNodeIdsByLabel(labelId: LabelId): Iterator[NodeId] = {
    val prefix = DistributedKeyConverter.toNodeKey(labelId)
    val iter = db.scanPrefix(prefix, BATCH_SIZE, true)

    new Iterator[NodeId] (){
      override def hasNext: Boolean = iter.hasNext
      override def next(): NodeId = {
        val data = iter.next()
        ByteUtils.getLong(data.getKey, prefix.length)
      }
    }
  }

  def deleteByLabel(labelId: LabelId): Unit =
    db.deleteRange(DistributedKeyConverter.toNodeKey(labelId, 0.toLong),
      DistributedKeyConverter.toNodeKey(labelId, -1.toLong))

  def delete(nodeId: NodeId, labelId: LabelId): Unit = db.delete(DistributedKeyConverter.toNodeKey(labelId, nodeId))

  def delete(nodeId:Long, labelIds: Array[LabelId]): Unit = labelIds.foreach(delete(nodeId, _))

  def delete(node: StoredNodeWithProperty): Unit = delete(node.id, node.labelIds)

  def batchPut(nodes: Seq[StoredNodeWithProperty]): Unit = {
    val res = nodes.flatMap(node => {
      if (node.labelIds.nonEmpty)
        node.labelIds.map(lid => (DistributedKeyConverter.toNodeKey(lid, node.id), NodeSerializer.serialize(node)))
      else
        Array((DistributedKeyConverter.toNodeKey(NONE_LABEL_ID, node.id), NodeSerializer.serialize(node)))
    })
    db.batchPut(res)
  }

  def batchDelete(keys: Seq[Array[Byte]]): Unit ={
    db.batchDelete(keys)
  }
}
