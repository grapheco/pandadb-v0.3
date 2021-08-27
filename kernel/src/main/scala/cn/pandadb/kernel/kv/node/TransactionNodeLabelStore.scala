package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.kv.KeyConverter.{LabelId, NodeId}
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import cn.pandadb.kernel.util.log.PandaLog
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{ReadOptions, Transaction, TransactionDB, WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:13 上午 2021/8/9
 * @Modified By:
 */
class TransactionNodeLabelStore(db: TransactionDB, logWriter: PandaLog) {
  val readOptions = new ReadOptions()

  def set(nodeId: NodeId, labelId: LabelId, tx: LynxTransaction): Unit = {
    val key = KeyConverter.toNodeLabelKey(nodeId, labelId)
    val ptx = tx.asInstanceOf[PandaTransaction]
    logWriter.writeUndoLog(ptx.id, DBNameMap.nodeLabelDB, key, null)
    ptx.rocksTxMap(DBNameMap.nodeLabelDB).put(key, Array.emptyByteArray)
  }

  def set(nodeId: NodeId, labels: Array[LabelId], tx: LynxTransaction): Unit = labels.foreach(set(nodeId, _, tx))

  def delete(nodeId: NodeId, labelId: LabelId, tx: LynxTransaction): Unit = {
    val key = KeyConverter.toNodeLabelKey(nodeId, labelId)
    val ptx = tx.asInstanceOf[PandaTransaction]
    logWriter.writeUndoLog(ptx.id, DBNameMap.nodeLabelDB, key, db.get(key))
    ptx.rocksTxMap(DBNameMap.nodeLabelDB).delete(key)
  }

  def delete(nodeId: NodeId, tx: LynxTransaction): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]
    val labelTx = ptx.rocksTxMap(DBNameMap.nodeLabelDB)
    getAllForLog(nodeId, labelTx).foreach(key => {
      logWriter.writeUndoLog(ptx.id, DBNameMap.nodeLabelDB, key, null)
    })

    val batch = new WriteBatch()
    getAll(nodeId, labelTx).foreach(lid => {
      batch.delete(KeyConverter.toNodeLabelKey(nodeId, lid))
    })

    labelTx.rebuildFromWriteBatch(batch)
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
    new Iterator[LabelId]() {
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(keyPrefix)

      override def next(): LabelId = {
        val label = ByteUtils.getInt(iter.key(), keyPrefix.length)
        iter.next()
        label
      }
    }.toArray
  }

  def getAllForLog(nodeId: NodeId, tx: Transaction): Array[Array[Byte]] = {
    val keyPrefix = KeyConverter.toNodeLabelKey(nodeId)
    val iter = tx.getIterator(readOptions)
    iter.seek(keyPrefix)
    new Iterator[Array[Byte]]() {
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(keyPrefix)

      override def next(): Array[Byte] = {
        val key = iter.key()
        iter.next()
        key
      }
    }.toArray
  }


  def getNodesCount(tx: Transaction): Long = {
    val iter = tx.getIterator(readOptions)
    iter.seekToFirst()
    var count: Long = 0
    var currentNode: Long = 0
    while (iter.isValid) {
      val id = ByteUtils.getLong(iter.key(), 0)
      if (currentNode != id) {
        currentNode = id
        count += 1
      }
      iter.next()
    }
    count
  }
}
