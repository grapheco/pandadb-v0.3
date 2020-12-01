package cn.pandadb.kernel.kv

import cn.pandadb.kernel.store.{StoredRelationWithProperty}
import org.rocksdb.RocksDB

class InEdgeRelationIndexStore(db: RocksDB) {
  /**
   * Index
   * ------------------------
   * key      |  value
   * ------------------------
   * srcId + EdgeType |  DestId
   * ------------------------
   * srcId + Category | DestId  necessary?
   * ------------------------
   */
  def setIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long, relationId: Long): Unit = {
    val keyBytes = KeyHandler.inEdgeKeyToBytes(fromNode, toNode, edgeType, category)
    db.put(keyBytes, KeyHandler.relationIdToBytes(relationId))
  }

  def deleteIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long): Unit = {
    val keyBytes = KeyHandler.inEdgeKeyToBytes(fromNode, toNode, edgeType, category)
    db.delete(keyBytes)
  }

  //////
  def getToNodesByFromNodeAndEdgeType(fromNode: Long, edgeType: Int): Iterator[Long] = {
    new GetToNodesByFromNodeAndEdgeType(fromNode, edgeType)
  }

  class GetToNodesByFromNodeAndEdgeType(fromNode: Long, edgeType: Int) extends Iterator[Long] {
    val prefix = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(fromNode, edgeType)
    val iter = db.newIterator()
    iter.seek(KeyHandler.inEdgeToBytes())

    override def hasNext: Boolean = iter.isValid && iter.key().slice(1, 13).sameElements(prefix)

    override def next(): Long = {
      val toNodeId = ByteUtils.getLong(iter.key(), 21)
      iter.next()
      toNodeId
    }
  }

  //////

}

class OutEdgeRelationIndexStore(db: RocksDB) {

  def setIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long, relationId: Long): Unit = {
    val keyBytes = KeyHandler.outEdgeKeyToBytes(fromNode, toNode, edgeType, category)
    db.put(keyBytes, KeyHandler.relationIdToBytes(relationId))
  }

  def deleteIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long): Unit = {
    val keyBytes = KeyHandler.outEdgeKeyToBytes(fromNode, toNode, edgeType, category)
    db.delete(keyBytes)
  }

  //////
  def getFromNodesByToNodeAndEdgeType(toNode: Long, edgeType: Int): Iterator[Long] = {
    new GetFromNodesByToNodeAndEdgeType(toNode, edgeType)
  }

  class GetFromNodesByToNodeAndEdgeType(toNode: Long, edgeType: Int) extends Iterator[Long] {
    val prefix = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(toNode, edgeType)
    val iter = db.newIterator()
    iter.seek(KeyHandler.inEdgeToBytes())

    override def hasNext: Boolean = iter.isValid && iter.key().slice(1, 13).sameElements(prefix)

    override def next(): Long = {
      val fromNode = ByteUtils.getLong(iter.key(), 21)
      iter.next()
      fromNode
    }
  }

  //////

}

class RelationStore(db: RocksDB) {
  def setRelation(relationId: Long, from: Long, to: Long, labelId: Int, category: Long, value: Map[String, Any]): Unit = {
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    val map = Map("from" -> from, "to" -> to, "labelId" -> labelId, "category" -> category, "prop" -> value)
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
        relation("labelId").asInstanceOf[Int], relation("prop").asInstanceOf[Map[String, Any]],
        relation("category").asInstanceOf[Long])
    }
    else throw new NoRelationGetException
  }

  def relationIsExist(relationId: Long): Boolean = {
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    val res = db.get(keyBytes)
    if (res != null) true else false
  }

  def getAll(): Iterator[StoredRelationWithProperty] = {
    new GetAllRocksRelation(db)
  }

  def close(): Unit = {
    db.close()
  }

  class GetAllRocksRelation(db: RocksDB) extends Iterator[StoredRelationWithProperty] {
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
          relation("labelId").asInstanceOf[Int], relation("prop").asInstanceOf[Map[String, Any]],
          relation("category").asInstanceOf[Long]
        )
      }
      iter.next()
      nextRelation
    }
  }

}

class NoRelationGetException extends Exception {
  override def getMessage: String = "no such relation to get"
}

class NoRelationIndexGetException extends Exception {
  override def getMessage: String = "no such relation index to get"
}