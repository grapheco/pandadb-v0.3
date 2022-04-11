package org.grapheco.pandadb.kernel.distribute.relationship

import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.grapheco.pandadb.kernel.store.StoredRelation
import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-18 10:52
 */
object RelationDirection extends Enumeration {
  type RelationDirection = Value
  val IN = Value(0)
  val OUT = Value(1)
}

class RelationDirectionStore(db: DistributedKVAPI, direction: RelationDirection.Value) {
  implicit def ByteString2ArrayByte(data: ByteString) = data.toByteArray

  val BATCH_SIZE = 10000

  def getKey(relation: StoredRelation): Array[Byte] =
    DistributedKeyConverter.edgeKeyToBytes(relation.from, relation.typeId, relation.to, direction)

  def set(relation: StoredRelation): Unit = {
    val key = getKey(relation)
    db.put(key, ByteUtils.longToBytes(relation.id))
  }

  def delete(relation: StoredRelation): Unit = {
    db.delete(getKey(relation))
  }

  def deleteRange(startId: Long): Unit = {
    db.deleteRange(
      DistributedKeyConverter.edgeKeyPrefixToBytes(startId, 0, direction),
      DistributedKeyConverter.edgeKeyPrefixToBytes(startId, -1, direction)
    )
  }

  def deleteRange(startId: Long, typeId: Int): Unit = {
    db.deleteRange(
      DistributedKeyConverter.edgeKeyToBytes(startId, typeId, 0, direction),
      DistributedKeyConverter.edgeKeyToBytes(startId, typeId, -1, direction)
    )
  }

  def getRightNodeIds(leftId: Long): Iterator[Long] = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(leftId, direction)
    new RightIdIterator(db, prefix)
  }

  def getRightNodeIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType, direction)
    new RightIdIterator(db, prefix)
  }

  class RightIdIterator(db: DistributedKVAPI, prefix: Array[Byte]) extends Iterator[Long] {
    val iter = db.scanPrefix(prefix, BATCH_SIZE, true)
    val offset = DistributedKeyConverter.KEY_PREFIX_SIZE + DistributedKeyConverter.NODE_ID_SIZE + DistributedKeyConverter.TYPE_ID_SIZE

    override def hasNext: Boolean = iter.hasNext

    override def next(): Long = ByteUtils.getLong(iter.next().getKey, offset)
  }

  def getRelationId(node1: Long, edgeType: Int, node2: Long): Option[Long] = {
    val keyBytes = DistributedKeyConverter.edgeKeyToBytes(node1, edgeType, node2, direction)
    val value = db.get(keyBytes)
    if (value.nonEmpty) Some(ByteUtils.getLong(value, 0))
    else None
  }

  def getRelationIds(nodeId: Long): Iterator[Long] = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(nodeId, direction)
    new RelationIdIterator(db, prefix)
  }

  def getRelationIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType, direction)
    new RelationIdIterator(db, prefix)
  }

  class RelationIdIterator(db: DistributedKVAPI, prefix: Array[Byte]) extends Iterator[Long] {
    val iter = db.scanPrefix(prefix, BATCH_SIZE, false)

    override def hasNext: Boolean = iter.hasNext

    override def next(): Long = ByteUtils.getLong(iter.next().getValue, 0)
  }

  def getRelations(nodeId: Long): Iterator[StoredRelation] = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(nodeId, direction)
    new RelationIterator(db, prefix)
  }

  def getRelations(nodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType, direction)
    new RelationIterator(db, prefix)
  }

  def getRelation(leftId: Long, edgeType: Int, rightId: Long): Option[StoredRelation] = {
    val key = DistributedKeyConverter.edgeKeyToBytes(leftId, edgeType, rightId, direction)
    val values = db.get(key)
    if (values.isEmpty) None
    else {
      val id = ByteUtils.getLong(values, 0)
      if (direction == RelationDirection.IN) Option(StoredRelation(id, rightId, leftId, edgeType))
      else Option(StoredRelation(id, leftId, rightId, edgeType))
    }
  }

  // ================= new added ===============================
  def countRelations(nodeId: Long): Long ={
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(nodeId, direction)
    db.scanPrefix(prefix, 10000, true).length
  }
  def countRelations(nodeId: Long, edgeType: Int): Long = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(nodeId, edgeType, direction)
    db.scanPrefix(prefix, 10000, true).length
  }
  def getRelationsEndNodeId(startNodeId: Long): Iterator[Long] = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(startNodeId, direction)
    db.scanPrefix(prefix, 10000,true).map(kv => {
      val key = kv.getKey.toByteArray
      ByteUtils.getLong(key, 13)
    })
  }
  def getRelationsEndNodeId(startNodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = DistributedKeyConverter.edgeKeyPrefixToBytes(startNodeId, edgeType, direction)
    db.scanPrefix(prefix, 10000,true).map(kv => {
      val key = kv.getKey.toByteArray
      ByteUtils.getLong(key, 13)
    })
  }
  // ===========================================================
  class RelationIterator(db: DistributedKVAPI, prefix: Array[Byte]) extends Iterator[StoredRelation] {
    val iter = db.scanPrefix(prefix, BATCH_SIZE, false)
    override def hasNext: Boolean = iter.hasNext

    override def next(): StoredRelation = {
      val data = iter.next()
      val key = data.getKey
      val node1 = ByteUtils.getLong(key, 1)
      val relType = ByteUtils.getInt(key, 9)
      val node2 = ByteUtils.getLong(key, 13)
      val id = ByteUtils.getLong(data.getValue, 0)

      if (direction == RelationDirection.IN) StoredRelation(id, node2, node1, relType)
      else StoredRelation(id, node1, node2, relType)
    }
  }

  def all(): Iterator[StoredRelation] ={
    direction match {
      case RelationDirection.IN => new RelationIterator(db, Array(DistributedKeyConverter.inRelationPrefix))
      case RelationDirection.OUT => new RelationIterator(db, Array(DistributedKeyConverter.outRelationPrefix))
    }
  }
}
