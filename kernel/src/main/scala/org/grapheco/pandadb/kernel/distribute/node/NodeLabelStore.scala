package org.grapheco.pandadb.kernel.distribute.node

import org.grapheco.pandadb.kernel.distribute.DistributedKeyConverter.{LabelId, NodeId}
import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-16 13:47
 */
class NodeLabelStore(db: DistributedKVAPI) {
  implicit def ByteString2ArrayByte(data: ByteString) = data.toByteArray

  val BATCH_SIZE = 10000

  def set(nodeId: NodeId, labelId: LabelId): Unit =
    db.put(DistributedKeyConverter.toNodeLabelKey(nodeId, labelId), Array.emptyByteArray)

  def set(nodeId: NodeId, labels: Array[LabelId]): Unit = labels.foreach(set(nodeId, _))

  def delete(nodeId: NodeId, labelId: LabelId): Unit =
    db.delete(DistributedKeyConverter.toNodeLabelKey(nodeId, labelId))

  def delete(nodeId: NodeId): Unit =
    db.deleteRange(DistributedKeyConverter.toNodeLabelKey(nodeId, 0),
      DistributedKeyConverter.toNodeLabelKey(nodeId, -1))

  def deleteRange(startKey: Array[Byte], endKey: Array[Byte]): Unit ={
    db.deleteRange(startKey, endKey)
  }

  def batchDelete(nodeIds: Seq[Long]): Unit ={
    val keys = nodeIds.map(id => DistributedKeyConverter.toNodeLabelKey(id))
    db.batchDelete(keys)
  }

  def get(nodeId: NodeId): Option[LabelId] = {
    val keyPrefix = DistributedKeyConverter.toNodeLabelKey(nodeId)
    val iter = db.scanPrefix(keyPrefix, BATCH_SIZE, true)
    if (iter.nonEmpty){
      Option(DistributedKeyConverter.getLabelIdInNodeLabelKey(iter.next().getKey))
    }
    else None
  }

  def exist(nodeId: NodeId, label: LabelId): Boolean = {
    val key = DistributedKeyConverter.toNodeLabelKey(nodeId, label)
    db.get(key).nonEmpty
  }

  def getAll(nodeId: NodeId): Array[LabelId] = {
    val keyPrefix = DistributedKeyConverter.toNodeLabelKey(nodeId)
    val iter = db.scanPrefix(keyPrefix, BATCH_SIZE, true)
    iter.map(f => DistributedKeyConverter.getLabelIdInNodeLabelKey(f.getKey)).toArray
  }


  def getNodesCount: Long = {
    val iter = db.scanPrefix(Array(DistributedKeyConverter.nodeLabelKeyPrefix), BATCH_SIZE, true)
    var count:Long = 0
    var currentNode:Long = 0
    while (iter.hasNext){
      val data = iter.next()
      val id = ByteUtils.getLong(data.getKey, DistributedKeyConverter.KEY_PREFIX_SIZE)
      if (currentNode != id){
        currentNode = id
        count += 1
      }
    }
    count
  }
}
