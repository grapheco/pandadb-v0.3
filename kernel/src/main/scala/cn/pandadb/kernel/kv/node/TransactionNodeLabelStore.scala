package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.kv.KeyConverter.{LabelId, NodeId}
import org.rocksdb.{Transaction, TransactionDB, WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:13 上午 2021/8/9
 * @Modified By:
 */
class TransactionNodeLabelStore(db: TransactionDB) {
  def set(nodeId: NodeId, labelId: LabelId, tx: Transaction): Unit =
    tx.put(KeyConverter.toNodeLabelKey(nodeId, labelId), Array.emptyByteArray)

  def set(nodeId: NodeId, labels: Array[LabelId], tx: Transaction): Unit = labels.foreach(set(nodeId, _, tx))

  def delete(nodeId: NodeId, labelId: LabelId, tx: Transaction): Unit =
    tx.delete(KeyConverter.toNodeLabelKey(nodeId, labelId))

  def delete(nodeId: NodeId, tx: Transaction): Unit ={
    this.synchronized{
      val batch = new WriteBatch()
      batch.deleteRange(KeyConverter.toNodeLabelKey(nodeId, 0),
        KeyConverter.toNodeLabelKey(nodeId, -1))
      tx.rebuildFromWriteBatch(batch)
    }
  }

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
