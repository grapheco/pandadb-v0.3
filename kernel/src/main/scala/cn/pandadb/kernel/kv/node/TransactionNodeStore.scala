package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.kv.KeyConverter.{LabelId, NodeId}
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import cn.pandadb.kernel.util.log.{PandaLog}
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{ReadOptions, Transaction, TransactionDB, WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:12 上午 2021/8/9
 * @Modified By:
 */
class TransactionNodeStore(db: TransactionDB, logWriter:PandaLog) {
  // [labelId,nodeId]->[Node]
  val NONE_LABEL_ID: Int = 0
  val readOptions = new ReadOptions()

  def set(nodeId: NodeId, labelIds: Array[LabelId], value: Array[Byte], tx: LynxTransaction): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]

    if (labelIds.nonEmpty)
      labelIds.foreach(labelId => {
        val key = KeyConverter.toNodeKey(labelId, nodeId)
        logWriter.writeUndoLog(ptx.id, DBNameMap.nodeDB, key, db.get(key))
        ptx.rocksTxMap(DBNameMap.nodeDB).put(key, value)
      })
    else {
      val key = KeyConverter.toNodeKey(NONE_LABEL_ID, nodeId)
      logWriter.writeUndoLog(ptx.id, DBNameMap.nodeDB, key, db.get(key))

      ptx.rocksTxMap(DBNameMap.nodeDB).put(key, value)
    }
  }

  def set(labelId: LabelId, node: StoredNodeWithProperty, tx: LynxTransaction): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]
    val key = KeyConverter.toNodeKey(labelId, node.id)
    logWriter.writeUndoLog(ptx.id, DBNameMap.nodeDB, key, db.get(key))

    ptx.rocksTxMap(DBNameMap.nodeDB).put(key, NodeSerializer.serialize(node))
  }

  def set(node: StoredNodeWithProperty, tx: LynxTransaction): Unit =
    set(node.id, node.labelIds, NodeSerializer.serialize(node), tx)

  def get(nodeId: NodeId, labelId: LabelId, tx: Transaction): Option[StoredNodeWithProperty] = {
    val value = tx.get(readOptions, KeyConverter.toNodeKey(labelId, nodeId))
    if(value != null) Some(NodeSerializer.deserializeNodeValue(value))
    else None
  }

  def all(tx: Transaction) : Iterator[StoredNodeWithProperty] = {
    val iter = tx.getIterator(readOptions)
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

  def getNodesByLabel(labelId: LabelId, tx: Transaction): Iterator[StoredNodeWithProperty] = {
    val iter = tx.getIterator(readOptions)
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
  def getNodesByLabelForLog(labelId: LabelId, tx: Transaction): Iterator[(Array[Byte], Array[Byte])] = {
    val iter = tx.getIterator(readOptions)
    val prefix = KeyConverter.toNodeKey(labelId)
    iter.seek(prefix)

    new Iterator[(Array[Byte], Array[Byte])] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
      override def next(): (Array[Byte], Array[Byte]) = {
        val key = iter.key()
        val value = iter.value()
        iter.next()
        (key, value)
      }
    }
  }

  def getNodeIdsByLabel(labelId: LabelId, tx: Transaction): Iterator[NodeId] = {
    val iter = tx.getIterator(readOptions)
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

  def getNodesByLabelWithoutDeserialize(labelId: LabelId, tx: Transaction): Iterator[NodeId] = {
    val iter = tx.getIterator(readOptions)
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

  def deleteByLabel(labelId: LabelId, tx: LynxTransaction): Unit = {
      val ptx = tx.asInstanceOf[PandaTransaction]
      getNodesByLabelForLog(labelId, ptx.rocksTxMap(DBNameMap.nodeDB)).foreach(kv => {
        logWriter.writeUndoLog(ptx.id, DBNameMap.nodeDB, kv._1, kv._2)
      })

      val batch = new WriteBatch()
      batch.deleteRange(KeyConverter.toNodeKey(labelId, 0.toLong),
        KeyConverter.toNodeKey(labelId, -1.toLong))
      ptx.rocksTxMap(DBNameMap.nodeDB).rebuildFromWriteBatch(batch)

  }


  def delete(nodeId: NodeId, labelId: LabelId, tx: LynxTransaction): Unit = {
    val key = KeyConverter.toNodeKey(labelId, nodeId)

    val ptx = tx.asInstanceOf[PandaTransaction]
    logWriter.writeUndoLog(ptx.id, DBNameMap.nodeDB, key, db.get(key))

    ptx.rocksTxMap(DBNameMap.nodeDB).delete(key)
  }


  def delete(nodeId:Long, labelIds: Array[LabelId], tx: LynxTransaction): Unit = labelIds.foreach(delete(nodeId, _, tx))

  def delete(node: StoredNodeWithProperty, tx: LynxTransaction): Unit = delete(node.id, node.labelIds, tx)
}
