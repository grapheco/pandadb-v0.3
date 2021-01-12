package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.KeyConverter.{LabelId, NodeId}
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import org.rocksdb.RocksDB


class NodeLabelStore(db: RocksDB)  {

  def set(nodeId: NodeId, labelId: LabelId): Unit =
    db.put(KeyConverter.toNodeLabelKey(nodeId, labelId), Array.emptyByteArray)

  def set(nodeId: NodeId, labels: Array[LabelId]): Unit = labels.foreach(set(nodeId, _))

  def delete(nodeId: NodeId, labelId: LabelId): Unit =
    db.delete(KeyConverter.toNodeLabelKey(nodeId, labelId))

  def delete(nodeId: NodeId): Unit =
    db.deleteRange(KeyConverter.toNodeLabelKey(nodeId, 0),
      KeyConverter.toNodeLabelKey(nodeId, -1))

  def get(nodeId: NodeId): Option[LabelId] = {
    val keyPrefix = KeyConverter.toNodeLabelKey(nodeId)
    val iter = db.newIterator()
    iter.seek(keyPrefix)
    if (iter.isValid && iter.key().startsWith(keyPrefix)) Some(KeyConverter.getLabelIdInNodeLabelKey(iter.key()))
    else None
  }

  def exist(nodeId: NodeId, label: LabelId): Boolean = {
    val key = KeyConverter.toNodeLabelKey(nodeId, label)
    db.get(key)!=null
  }

  def getAll(nodeId: NodeId): Array[LabelId] = {
    val keyPrefix = KeyConverter.toNodeLabelKey(nodeId)
    val iter = db.newIterator()
    iter.seek(keyPrefix)
    new Iterator[LabelId] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(keyPrefix)

      override def next(): LabelId = {
        val label = ByteUtils.getInt(iter.key(), keyPrefix.length)
        iter.next()
        label
      }
    }.toArray
  }


  def getNodesCount: Long = {
    val iter = db.newIterator()
    iter.seekToFirst()
    var count:Long = 0
    var currentNode:Long = 0
    while (iter.isValid){
      val id = ByteUtils.getLong(iter.key(), 0)
      if (currentNode != id){
        currentNode = id
        count +=1
      }
      iter.next()
    }
    count
  }
}
