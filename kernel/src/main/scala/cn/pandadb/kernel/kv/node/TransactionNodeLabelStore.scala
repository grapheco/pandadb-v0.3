package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.kv.KeyConverter.{LabelId, NodeId}
import org.rocksdb.{ReadOptions, Transaction, TransactionDB, WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:13 上午 2021/8/9
 * @Modified By:
 */
class TransactionNodeLabelStore(db: TransactionDB) {
  val readOptions = new ReadOptions()

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

  def get(nodeId: NodeId, tx: Transaction): Option[LabelId] = {
    val keyPrefix = KeyConverter.toNodeLabelKey(nodeId)
    val iter = tx.getIterator(readOptions)
    iter.seek(keyPrefix)
    if (iter.isValid && iter.key().startsWith(keyPrefix)) Some(KeyConverter.getLabelIdInNodeLabelKey(iter.key()))
    else None
  }

  def exist(nodeId: NodeId, label: LabelId, tx: Transaction): Boolean = {
    val key = KeyConverter.toNodeLabelKey(nodeId, label)
    tx.get(readOptions, key) != null
  }

  def getAll(nodeId: NodeId, tx: Transaction): Array[LabelId] = {
    val keyPrefix = KeyConverter.toNodeLabelKey(nodeId)
    val iter = tx.getIterator(readOptions)
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


  def getNodesCount(tx: Transaction): Long = {
    val iter = tx.getIterator(readOptions)
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
