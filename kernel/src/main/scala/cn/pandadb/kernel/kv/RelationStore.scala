package cn.pandadb.kernel.kv

import cn.pandadb.kernel.store.{StoredRelationWithProperty}
import org.rocksdb.RocksDB

class InEdgeRelationIndexStore(db: RocksDB) {
  /**
   * Index
   * ------------------------
   * key      |  Find
   * ------------------------
   * srcId + EdgeType |  DestId
   * ------------------------
   * srcId + Category | DestId
   * ------------------------
   * srcId + EdgeType + Category | DestId
   * ------------------------
   */
  def setIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long, relationId: Long): Unit = {
    val keyBytes = KeyHandler.inEdgeKeyToBytes(fromNode, toNode, edgeType, category)
    db.put(keyBytes, ByteUtils.longToBytes(relationId))
  }

  def deleteIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long): Unit = {
    val keyBytes = KeyHandler.inEdgeKeyToBytes(fromNode, toNode, edgeType, category)
    db.delete(keyBytes)
  }

  //////
  type SrcId = Long
  type CategoryId = Long
  type EdgeType = Int

  def getAllToNodes(fromNode: SrcId, edgeType: EdgeType): Iterator[Long] = {
    new Iterator[Long] {
      val prefix = KeyHandler.relationIndexPrefixKeyToBytes(KeyHandler.KeyType.InEdge.id.toByte, fromNode, edgeType)
      val iter = db.newIterator()
      iter.seek(prefix)

      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

      override def next(): Long = {
        val toNodeId = ByteUtils.getLong(iter.key(), 21)
        iter.next()
        toNodeId
      }
    }
  }

  def getAllToNodes(fromNode: SrcId, category: CategoryId): Iterator[Long] = {
    new Iterator[Long] {
      val categoryBytes = ByteUtils.longToBytes(category)
      val prefix = KeyHandler.relationIndexPrefixKeyToBytes(KeyHandler.KeyType.InEdge.id.toByte, fromNode)
      val iter = db.newIterator()
      iter.seek(prefix)
      var toNodeId: Long = _

      override def hasNext: Boolean = {
        var isFinish = false
        var isFound = false
        if (iter.isValid && iter.key().startsWith(prefix)) {
          while (!isFinish) {
            if (iter.key().slice(13, 21).sameElements(categoryBytes)) {
              isFinish = true
              isFound = true
              toNodeId = ByteUtils.getLong(iter.key(), 21)
              iter.next()
            }
            else {
              if (iter.isValid && iter.key().startsWith(prefix)) {
                iter.next()
              }
              else {
                isFinish = true
                isFound = false
              }
            }
          }
          isFound
        }
        else false
      }

      override def next(): Long = {
        toNodeId
      }
    }
  }

  def getAllToNodes(fromNode: SrcId, edgeType: EdgeType, category: CategoryId): Iterator[Long] = {
    new Iterator[Long] {
      val prefix = KeyHandler.relationIndexPrefixKeyToBytes(KeyHandler.KeyType.InEdge.id.toByte, fromNode, edgeType, category)
      val iter = db.newIterator()
      iter.seek(prefix)

      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

      override def next(): Long = {
        val toNodeId = ByteUtils.getLong(iter.key(), 21)
        iter.next()
        toNodeId
      }
    }
  }

  //////

}

class OutEdgeRelationIndexStore(db: RocksDB) {

  def setIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long, relationId: Long): Unit = {
    val keyBytes = KeyHandler.outEdgeKeyToBytes(fromNode, toNode, edgeType, category)
    db.put(keyBytes, ByteUtils.longToBytes(relationId))
  }

  def deleteIndex(fromNode: Long, edgeType: Int, category: Long, toNode: Long): Unit = {
    val keyBytes = KeyHandler.outEdgeKeyToBytes(fromNode, toNode, edgeType, category)
    db.delete(keyBytes)
  }

  //////
  type SrcId = Long
  type CategoryId = Long
  type EdgeType = Int

  def getAllFromNodes(toNode: SrcId, edgeType: EdgeType): Iterator[Long] = {
    new Iterator[Long] {
      val prefix = KeyHandler.relationIndexPrefixKeyToBytes(KeyHandler.KeyType.OutEdge.id.toByte, toNode, edgeType)
      val iter = db.newIterator()
      iter.seek(prefix)
      override def hasNext: Boolean = {
        iter.isValid && iter.key().startsWith(prefix)
      }

      override def next(): Long = {
        val toNodeId = ByteUtils.getLong(iter.key(), 21)
        iter.next()
        toNodeId
      }
    }
  }

  def getAllFromNodes(toNode: SrcId, category: CategoryId): Iterator[Long] = {
    new Iterator[Long] {
      val categoryBytes = ByteUtils.longToBytes(category)

      val prefix = KeyHandler.relationIndexPrefixKeyToBytes(KeyHandler.KeyType.OutEdge.id.toByte, toNode)
      val iter = db.newIterator()
      iter.seek(prefix)
      var fromNodeId: Long = _

      override def hasNext: Boolean = {
        var isFinish = false
        var isFound = false
        if (iter.isValid && iter.key().startsWith(prefix)) {
          while (!isFinish) {
            if (iter.key().slice(13, 21).sameElements(categoryBytes)) {
              isFinish = true
              isFound = true
              fromNodeId = ByteUtils.getLong(iter.key(), 21)
              iter.next()
            }
            else {
              if (iter.isValid && iter.key().startsWith(prefix)) {
                iter.next()
              }
              else {
                isFinish = true
                isFound = false
              }
            }
          }
          isFound
        }
        else false
      }

      override def next(): Long = {
        fromNodeId
      }
    }
  }

  def getAllFromNodes(toNode: SrcId, edgeType: EdgeType, category: CategoryId): Iterator[Long] = {
    new Iterator[Long] {
      val prefix = KeyHandler.relationIndexPrefixKeyToBytes(KeyHandler.KeyType.OutEdge.id.toByte, toNode, edgeType, category)
      val iter = db.newIterator()
      iter.seek(prefix)

      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

      override def next(): Long = {
        val toNodeId = ByteUtils.getLong(iter.key(), 21)
        iter.next()
        toNodeId
      }
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
            relation("labelId").asInstanceOf[Int], relation("prop").asInstanceOf[Map[String, Any]],
            relation("category").asInstanceOf[Long]
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