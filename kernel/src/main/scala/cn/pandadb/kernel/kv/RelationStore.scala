package cn.pandadb.kernel.kv

import cn.pandadb.kernel.store.{StoredRelationWithProperty}
import org.rocksdb.RocksDB

class RelationInEdgeIndexStore(db: RocksDB) {
  ""/**""
   * in edge data structure
   * ------------------------
   * type(1Byte),toNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)-->relationValue(id, properties)
   * ------------------------
   */
  val RELATION_ID = "id"
  val RELATION_PROPERTY = "property"

  def setIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long,
               relationId: Long, property: Map[String, Any]=Map()): Unit = {

    val keyBytes = KeyHandler.inEdgeKeyToBytes(toNode, edgeType, category, fromNode)
    db.put(keyBytes, ByteUtils.mapToBytes(Map[String, Any](RELATION_ID->relationId, RELATION_PROPERTY->property)))
  }

  def deleteIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long): Unit = {
    val keyBytes = KeyHandler.inEdgeKeyToBytes(toNode, edgeType, category, fromNode)
    db.delete(keyBytes)
  }

  //////
  def findNodes(): Iterator[Long] = {
    val prefix = KeyHandler.inEdgeKeyPrefixToBytes()
    new NodesIterator(db, prefix)
  }

  def findNodes(toNodeId: Long): Iterator[Long] = {
    val prefix = KeyHandler.inEdgeKeyPrefixToBytes(toNodeId)
    new NodesIterator(db, prefix)
  }

  def findNodes(toNodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = KeyHandler.inEdgeKeyPrefixToBytes(toNodeId, edgeType)
    new NodesIterator(db, prefix)
  }

  def findNodes(toNodeId: Long, edgeType: Int, category: Long): Iterator[Long] = {
    val prefix = KeyHandler.inEdgeKeyPrefixToBytes(toNodeId, edgeType, category)
    new NodesIterator(db, prefix)
  }

  class NodesIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val fromNodeId = ByteUtils.getLong(iter.key(), 21)
      iter.next()
      fromNodeId
    }
  }
  //////

  def getRelations(): Iterator[StoredRelationWithProperty] = {
    val prefix = KeyHandler.inEdgeKeyPrefixToBytes()
    new RelationIterator(db, prefix)
  }

  def getRelations(toNodeId: Long): Iterator[StoredRelationWithProperty] = {
    val prefix = KeyHandler.inEdgeKeyPrefixToBytes(toNodeId)
    new RelationIterator(db, prefix)
  }

  def getRelations(toNodeId: Long, edgeType: Int): Iterator[StoredRelationWithProperty] = {
    val prefix = KeyHandler.inEdgeKeyPrefixToBytes(toNodeId, edgeType)
    new RelationIterator(db, prefix)
  }

  def getRelations(toNodeId: Long, edgeType: Int, category: Long): Iterator[StoredRelationWithProperty] = {
    val prefix = KeyHandler.inEdgeKeyPrefixToBytes(toNodeId, edgeType, category)
    new RelationIterator(db, prefix)
  }

  class RelationIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[StoredRelationWithProperty]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): StoredRelationWithProperty = {
      val values = ByteUtils.mapFromBytes(db.get(iter.key()))
      val keys = KeyHandler.parseInEdgeKeyFromBytes(iter.key())
      val relation = new StoredRelationWithProperty(values(RELATION_ID).asInstanceOf[Long], keys._4, keys._1, keys._2,
        keys._3.toInt, values(RELATION_PROPERTY).asInstanceOf[Map[Int, Any]])
      iter.next()
      relation
    }
  }
  //////
}

class RelationOutEdgeIndexStore(db: RocksDB) {
  /**
   * out edge data structure
   * ------------------------
   * type(1Byte),fromNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),toNodeId(8Bytes)-->relationValue(id, properties)
   * ------------------------
   */
  val RELATION_ID = "id"
  val RELATION_PROPERTY = "property"

  def setIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long,
               relationId: Long, property: Map[String, Any]=Map()): Unit = {

    val keyBytes = KeyHandler.outEdgeKeyToBytes(fromNode, edgeType, category, toNode)
    db.put(keyBytes, ByteUtils.mapToBytes(Map[String, Any](RELATION_ID->relationId, RELATION_PROPERTY->property)))
  }

  def deleteIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long): Unit = {
    val keyBytes = KeyHandler.outEdgeKeyToBytes(fromNode, edgeType, category, toNode)
    db.delete(keyBytes)
  }

  //////
  def findNodes(): Iterator[Long] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes()
    new NodesIterator(db, prefix)
  }

  def findNodes(fromNodeId: Long): Iterator[Long] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(fromNodeId)
    new NodesIterator(db, prefix)
  }

  def findNodes(fromNodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(fromNodeId, edgeType)
    new NodesIterator(db, prefix)
  }

  def findNodes(fromNodeId: Long, edgeType: Int, category: Long): Iterator[Long] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(fromNodeId, edgeType, category)
    new NodesIterator(db, prefix)
  }

  class NodesIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val toNodeId = ByteUtils.getLong(iter.key(), 21)
      iter.next()
      toNodeId
    }
  }
  //////

  def getRelations(): Iterator[StoredRelationWithProperty] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes()
    new RelationIterator(db, prefix)
  }

  def getRelations(fromNodeId: Long): Iterator[StoredRelationWithProperty] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(fromNodeId)
    new RelationIterator(db, prefix)
  }

  def getRelations(fromNodeId: Long, edgeType: Int): Iterator[StoredRelationWithProperty] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(fromNodeId, edgeType)
    new RelationIterator(db, prefix)
  }

  def getRelations(fromNodeId: Long, edgeType: Int, category: Long): Iterator[StoredRelationWithProperty] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(fromNodeId, edgeType, category)
    new RelationIterator(db, prefix)
  }

  class RelationIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[StoredRelationWithProperty]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): StoredRelationWithProperty = {
      val values = ByteUtils.mapFromBytes(db.get(iter.key()))
      val keys = KeyHandler.parseOutEdgeKeyFromBytes(iter.key())
      val relation = new StoredRelationWithProperty(values(RELATION_ID).asInstanceOf[Long],
        keys._1, keys._4, keys._2, keys._3.toInt, values(RELATION_PROPERTY).asInstanceOf[Map[Int, Any]])
      iter.next()
      relation
    }
  }
  //////
}

class RelationStore(db: RocksDB) {
  def setRelation(relationId: Long, from: Long, to: Long, labelId: Int, category: Long, value: Map[String, Any]): Unit = {
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    val map = Map("from" -> from, "to" -> to, "labelId" -> labelId, "category" -> category, "property" -> value)
    val valueBytes = ByteUtils.mapToBytes(map)
    db.put(keyBytes, valueBytes)
  }

  def deleteRelation(relationId: Long): Unit = {
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    db.delete(keyBytes)
  }

  def getRelation(relationId: Long): StoredRelationWithProperty = {
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    val res = db.get(keyBytes)
    if (res != null) {
      val relation = ByteUtils.mapFromBytes(res)
      new StoredRelationWithProperty(relationId, relation("from").asInstanceOf[Long], relation("to").asInstanceOf[Long],
        relation("labelId").asInstanceOf[Int],
        relation("category").asInstanceOf[Int], relation("property").asInstanceOf[Map[Int, Any]])
    }
    else throw new NoRelationGetException
  }

  def relationIsExist(relationId: Long): Boolean = {
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    val res = db.get(keyBytes)
    if (res != null) true else false
  }

  def getAll(): Iterator[StoredRelationWithProperty] = {
    new Iterator[StoredRelationWithProperty]{
      val keyPrefix = KeyHandler.relationKeyPrefix()
      val iter = db.newIterator()
      iter.seek(keyPrefix)

      override def hasNext: Boolean = {
        iter.isValid && iter.key().startsWith(keyPrefix)
      }

      override def next(): StoredRelationWithProperty = {
        val nextRelation = {
          val keyBytes = iter.key()
          val relation = ByteUtils.mapFromBytes(db.get(keyBytes))

          val relationId = ByteUtils.getLong(keyBytes, 1)
          new StoredRelationWithProperty(relationId, relation("from").asInstanceOf[Long], relation("to").asInstanceOf[Long],
            relation("labelId").asInstanceOf[Int], relation("category").asInstanceOf[Int],
            relation("property").asInstanceOf[Map[Int, Any]]
          )
        }
        iter.next()
        nextRelation
      }
    }
  }
  def close(): Unit = {
    db.close()
  }
}
class NoRelationGetException extends Exception {
  override def getMessage: String = "no such relation to get"
}

class NoRelationIndexGetException extends Exception {
  override def getMessage: String = "no such relation index to get"
}