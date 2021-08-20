package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.kv.relation.TransactionRelationDirection.{Direction, IN}
import cn.pandadb.kernel.store.StoredRelation
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import cn.pandadb.kernel.util.log.PandaLog
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{ReadOptions, Transaction, TransactionDB, WriteBatch}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:14 上午 2021/8/9
 * @Modified By:
 */
object TransactionRelationDirection extends Enumeration {
  type Direction = Value
  val IN = Value (0)
  val OUT = Value (1)
}

class TransactionRelationDirectionStore(db: TransactionDB, DIRECTION: Direction, logWriter: PandaLog) {
  val dbName = {
    DIRECTION match {
      case TransactionRelationDirection.IN => DBNameMap.inRelationDB
      case TransactionRelationDirection.OUT => DBNameMap.outRelationDB
    }
  }
  /**
   * in edge data structure
   * ------------------------
   * type(1Byte),nodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)-->relationValue(id, properties)
   * ------------------------
   */
  val readOptions = new ReadOptions()

  def getKey(relation: StoredRelation): Array[Byte] =
    if (DIRECTION == IN) KeyConverter.edgeKeyToBytes(relation.to, relation.typeId, relation.from)
    else                 KeyConverter.edgeKeyToBytes(relation.from, relation.typeId, relation.to)

  def set(relation: StoredRelation, tx: LynxTransaction): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]
    val keyBytes = getKey(relation)
    logWriter.writeUndoLog(ptx.id, dbName, keyBytes, db.get(keyBytes))
    ptx.rocksTxMap(dbName).put(keyBytes, ByteUtils.longToBytes(relation.id))
  }

  def delete(relation: StoredRelation, tx: LynxTransaction): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]
    val keyBytes = getKey(relation)
    logWriter.writeUndoLog(ptx.id, dbName, keyBytes, db.get(keyBytes))
    ptx.rocksTxMap(dbName).delete(keyBytes)
  }

  def deleteRange(firstId: Long, tx: LynxTransaction): Unit = {
    this.synchronized{
      val ptx = tx.asInstanceOf[PandaTransaction]
      val thisTx = ptx.rocksTxMap(dbName)
      getRelationIdsForLog(firstId, thisTx).foreach(kv => logWriter.writeUndoLog(ptx.id, dbName, kv._1, kv._2))

      val batch = new WriteBatch()
      batch.deleteRange(KeyConverter.edgeKeyPrefixToBytes(firstId,0),
        KeyConverter.edgeKeyPrefixToBytes(firstId, -1))
      thisTx.rebuildFromWriteBatch(batch)
    }
  }

  def deleteRange(firstId: Long, typeId: Int, tx: LynxTransaction): Unit = {
    this.synchronized{
      val ptx = tx.asInstanceOf[PandaTransaction]
      val thisTx = ptx.rocksTxMap(dbName)
      getRelationIdsForLog(firstId, typeId, thisTx).foreach(kv => logWriter.writeUndoLog(ptx.id, dbName, kv._1, kv._2))

      val batch = new WriteBatch()
      batch.deleteRange(KeyConverter.edgeKeyToBytes(firstId, typeId, 0),
        KeyConverter.edgeKeyToBytes(firstId, typeId, -1))
      thisTx.rebuildFromWriteBatch(batch)
    }
  }

  def get(node1: Long, edgeType: Int, node2: Long, tx: Transaction): Option[Long] = {
    val keyBytes = KeyConverter.edgeKeyToBytes(node1, edgeType, node2)
    val value = tx.get(readOptions, keyBytes)
    if (value!=null)
      Some(ByteUtils.getLong(value, 0))
    else
      None
  }

  def getNodeIds(nodeId: Long, tx: Transaction): Iterator[Long] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId)
    new NodeIdIterator(tx, prefix)
  }

  def getNodeIds(nodeId: Long, edgeType: Int, tx: Transaction): Iterator[Long] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType)
    new NodeIdIterator(tx, prefix)
  }

  class NodeIdIterator(tx: Transaction, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = tx.getIterator(readOptions)
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val fromNodeId = ByteUtils.getLong(iter.key(), 12)
      iter.next()
      fromNodeId
    }
  }

  def getRelationIds(nodeId: Long, tx: Transaction): Iterator[Long] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId)
    new RelationIdIterator(tx, prefix)
  }
  def getRelationIdsForLog(nodeId: Long, tx: Transaction): Iterator[(Array[Byte], Array[Byte])] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId)
    new RelationIdIteratorForLog(tx, prefix)
  }

  def getRelationIds(nodeId: Long, edgeType: Int, tx: Transaction): Iterator[Long] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType)
    new RelationIdIterator(tx, prefix)
  }
  def getRelationIdsForLog(nodeId: Long, edgeType: Int, tx: Transaction): Iterator[(Array[Byte], Array[Byte])] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType)
    new RelationIdIteratorForLog(tx, prefix)
  }

  class RelationIdIterator(tx: Transaction, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = tx.getIterator(readOptions)
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val id = ByteUtils.getLong(iter.value(), 0)
      iter.next()
      id
    }
  }

  class RelationIdIteratorForLog(tx: Transaction, prefix: Array[Byte]) extends Iterator[(Array[Byte], Array[Byte])]{
    val iter = tx.getIterator(readOptions)
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): (Array[Byte], Array[Byte]) = {
      val key = iter.key()
      val value = iter.value()
      iter.next()
      (key, value)
    }
  }

  def getRelations(nodeId: Long, tx: Transaction): Iterator[StoredRelation] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId)
    new RelationIterator(tx, prefix)
  }

  def getRelations(nodeId: Long, edgeType: Int, tx: Transaction): Iterator[StoredRelation] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType)
    new RelationIterator(tx, prefix)
  }

  def getRelation(firstNodeId: Long, edgeType: Int, secondNodeId: Long, tx: Transaction): Option[StoredRelation] = {
    val key = KeyConverter.edgeKeyToBytes(firstNodeId, edgeType, secondNodeId)
    val values = tx.get(readOptions, key)
    if (values == null) None
    else {
      val id = ByteUtils.getLong(values, 0)
      if(DIRECTION==IN) Option(StoredRelation(id, secondNodeId, firstNodeId, edgeType))
      else              Option(StoredRelation(id, firstNodeId, secondNodeId, edgeType))
    }
  }

  class RelationIterator(tx: Transaction, prefix: Array[Byte]) extends Iterator[StoredRelation]{
    val iter = tx.getIterator(readOptions)
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): StoredRelation = {
      val key = iter.key()
      val node1 = ByteUtils.getLong(key, 0)
      val relType = ByteUtils.getInt(key, 8)
      val node2  = ByteUtils.getLong(key, 12)
      val id = ByteUtils.getLong(iter.value(), 0)

      val res =
        if(DIRECTION==IN) StoredRelation(id, node2, node1, relType)
        else              StoredRelation(id, node1, node2, relType)
      iter.next()
      res
    }
  }

  def all(tx: Transaction): Iterator[StoredRelation] = {
    new RelationIterator(tx, Array.emptyByteArray)
  }

  def close(): Unit = {
    db.close()
  }
}
