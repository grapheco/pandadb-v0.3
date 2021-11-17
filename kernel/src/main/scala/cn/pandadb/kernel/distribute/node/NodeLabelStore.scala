package cn.pandadb.kernel.distribute.node

import cn.pandadb.kernel.distribute.DistributedKeyConverter.{LabelId, NodeId}
import cn.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import cn.pandadb.kernel.kv.ByteUtils

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-16 13:47
 */
class NodeLabelStore(db: DistributedKVAPI) {
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

//  def batchDelete(nodeIds: Seq[Long]): Unit ={
//    val keys = nodeIds.map(id => DistributedKeyConverter.toNodeLabelKey(id))
//    db.batchDelete(keys)
//  }

  def get(nodeId: NodeId): Option[LabelId] = {
    val keyPrefix = DistributedKeyConverter.toNodeLabelKey(nodeId)
    val iter = db.scanPrefix(keyPrefix, true)
    if (iter.nonEmpty){
      Option(DistributedKeyConverter.getLabelIdInNodeLabelKey(iter.next()._1))
    }
    else None
  }

  def exist(nodeId: NodeId, label: LabelId): Boolean = {
    val key = DistributedKeyConverter.toNodeLabelKey(nodeId, label)
    db.get(key).nonEmpty
  }

  def getAll(nodeId: NodeId): Array[LabelId] = {
    val keyPrefix = DistributedKeyConverter.toNodeLabelKey(nodeId)
    val iter = db.scanPrefix(keyPrefix, true)
    iter.map(f => DistributedKeyConverter.getLabelIdInNodeLabelKey(f._1)).toArray
  }


  def getNodesCount: Long = {
    val iter = db.scanPrefix(Array(DistributedKeyConverter.nodeLabelKeyPrefix), true)
    var count:Long = 0
    var currentNode:Long = 0
    while (iter.hasNext){
      val data = iter.next()
      val id = ByteUtils.getLong(data._1, DistributedKeyConverter.KEY_PREFIX_SIZE)
      if (currentNode != id){
        currentNode = id
        count += 1
      }
    }
    count
  }
}
