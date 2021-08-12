package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.kv.relation.TransactionRelationDirection.{Direction, IN}
import cn.pandadb.kernel.store.StoredRelation
import org.rocksdb.{Transaction, TransactionDB, WriteBatch}

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

class TransactionRelationDirectionStore(db: TransactionDB, DIRECTION: Direction) {
  /**
   * in edge data structure
   * ------------------------
   * type(1Byte),nodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)-->relationValue(id, properties)
   * ------------------------
   */

  def getKey(relation: StoredRelation): Array[Byte] =
    if (DIRECTION == IN) KeyConverter.edgeKeyToBytes(relation.to, relation.typeId, relation.from)
    else                 KeyConverter.edgeKeyToBytes(relation.from, relation.typeId, relation.to)

  def set(relation: StoredRelation, tx: Transaction): Unit = {
    val keyBytes = getKey(relation)
    tx.put(keyBytes, ByteUtils.longToBytes(relation.id))
  }

  def delete(relation: StoredRelation, tx: Transaction): Unit = {
    val keyBytes = getKey(relation)
    tx.delete(keyBytes)
  }

  def deleteRange(firstId: Long, tx: Transaction): Unit = {
    this.synchronized{
      val batch = new WriteBatch()
      batch.deleteRange(KeyConverter.edgeKeyPrefixToBytes(firstId,0),
        KeyConverter.edgeKeyPrefixToBytes(firstId, -1))
      tx.rebuildFromWriteBatch(batch)
    }
  }

  def deleteRange(firstId: Long, typeId: Int, tx: Transaction): Unit = {
    this.synchronized{
      val batch = new WriteBatch()
      batch.deleteRange(KeyConverter.edgeKeyToBytes(firstId, typeId, 0),
        KeyConverter.edgeKeyToBytes(firstId, typeId, -1))
      tx.rebuildFromWriteBatch(batch)
    }
  }

  def get(node1: Long, edgeType: Int, node2: Long): Option[Long] = {
    val keyBytes = KeyConverter.edgeKeyToBytes(node1, edgeType, node2)
    val value = db.get(keyBytes)
    if (value!=null)
      Some(ByteUtils.getLong(value, 0))
    else
      None
  }

  def getNodeIds(nodeId: Long): Iterator[Long] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId)
    new NodeIdIterator(db, prefix)
  }

  def getNodeIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType)
    new NodeIdIterator(db, prefix)
  }

  class NodeIdIterator(db: TransactionDB, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val fromNodeId = ByteUtils.getLong(iter.key(), 12)
      iter.next()
      fromNodeId
    }
  }

  def getRelationIds(nodeId: Long): Iterator[Long] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId)
    new RelationIdIterator(db, prefix)
  }

  def getRelationIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType)
    new RelationIdIterator(db, prefix)
  }

  class RelationIdIterator(db: TransactionDB, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val id = ByteUtils.getLong(iter.value(), 0)
      iter.next()
      id
    }
  }

  def getRelations(nodeId: Long): Iterator[StoredRelation] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId)
    new RelationIterator(db, prefix)
  }

  def getRelations(nodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    val prefix = KeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType)
    new RelationIterator(db, prefix)
  }

  def getRelation(firstNodeId: Long, edgeType: Int, secondNodeId: Long): Option[StoredRelation] = {
    val key = KeyConverter.edgeKeyToBytes(firstNodeId, edgeType, secondNodeId)
    val values = db.get(key)
    if (values == null) None
    else {
      val id = ByteUtils.getLong(values, 0)
      if(DIRECTION==IN) Option(StoredRelation(id, secondNodeId, firstNodeId, edgeType))
      else              Option(StoredRelation(id, firstNodeId, secondNodeId, edgeType))
    }
  }

  class RelationIterator(db: TransactionDB, prefix: Array[Byte]) extends Iterator[StoredRelation]{
    val iter = db.newIterator()
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

  def all(): Iterator[StoredRelation] = {
    new RelationIterator(db, Array.emptyByteArray)
  }

  def close(): Unit = {
    db.close()
  }
}
