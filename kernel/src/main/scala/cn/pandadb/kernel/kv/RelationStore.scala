package cn.pandadb.kernel.kv

import cn.pandadb.kernel.store.{StoredRelationWithProperty}
import org.rocksdb.RocksDB

class InEdgeRelationStore(db: RocksDB){
  def setRelation(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long, valueMap: Map[String, Any]): Unit = {
    val key = KeyHandler.inEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val value = ByteUtils.mapToBytes(valueMap)
    db.put(key, value)
  }

  def getRelationValueMap(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long): Map[String, Any] = {
    val key = KeyHandler.inEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val mapBytes = db.get(key)
    ByteUtils.mapFromBytes(mapBytes)
  }

  def relationIsExist(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long): Boolean = {
    val key = KeyHandler.inEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val res = db.get(key)
    if (res == null) false else true
  }

  def deleteRelation(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long): Unit = {
    val key = KeyHandler.inEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    db.delete(key)
  }

  def getAllRelation(): Iterator[StoredRelationWithProperty] = {
    new GetAllRocksInRelation(db)
  }

  def close(): Unit = {
    db.close()
  }

  class GetAllRocksInRelation(db: RocksDB) extends Iterator[StoredRelationWithProperty] {
    val prefix1 = new Array[Byte](1)
    val in = KeyHandler.KeyType.InEdge.id.toByte
    ByteUtils.setByte(prefix1, 0, in)

    val inIter = db.newIterator()
    inIter.seek(prefix1)

    override def hasNext: Boolean = {
      inIter.isValid
    }

    override def next(): StoredRelationWithProperty = {
      val nextRelation = {
        val bytes = inIter.key()
        val from = ByteUtils.getLong(bytes, 1)
        val to = ByteUtils.getLong(bytes, 21)
        val label = ByteUtils.getInt(bytes, 9)
        val category =  ByteUtils.getInt(bytes, 13)
        val map = getRelationValueMap(from, to, label, category)

        new StoredRelationWithProperty(0, from, to, label, map, category)
      }
      nextRelation
    }
  }
}

class OutEdgeRelationStore(db: RocksDB){
  def setRelation(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long, valueMap: Map[String, Any]): Unit = {
    val key = KeyHandler.outEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val value = ByteUtils.mapToBytes(valueMap)
    db.put(key, value)
  }

  def getRelationValueMap(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long): Map[String, Any] = {
    val key = KeyHandler.outEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val mapBytes = db.get(key)
    ByteUtils.mapFromBytes(mapBytes)
  }

  def relationIsExist(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long): Boolean = {
    val key = KeyHandler.outEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    val res = db.get(key)
    if (res == null) false else true
  }

  def deleteRelation(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long): Unit = {
    val key = KeyHandler.outEdgeKeyToBytes(fromNodeId, toNodeId, labelId, category)
    db.delete(key)
  }

  def getAllRelation(): Iterator[StoredRelationWithProperty] = {
    new GetAllRocksOutRelation(db)
  }

  def close(): Unit = {
    db.close()
  }

  class GetAllRocksOutRelation(db: RocksDB) extends Iterator[StoredRelationWithProperty] {
    val prefix2 = new Array[Byte](1)
    val out = KeyHandler.KeyType.OutEdge.id.toByte
    ByteUtils.setByte(prefix2, 0, out)

    val outIter = db.newIterator()
    outIter.seek(prefix2)

    override def hasNext: Boolean = {
     outIter.isValid
    }

    override def next(): StoredRelationWithProperty = {
      val nextRelation = {
        val bytes = outIter.key()
        val from = ByteUtils.getLong(bytes, 21)
        val to = ByteUtils.getLong(bytes, 1)
        val label = ByteUtils.getInt(bytes, 9)
        val category =  ByteUtils.getInt(bytes, 13)
        val map = getRelationValueMap(from, to, label, category)

        new StoredRelationWithProperty(0, from, to, label, map, category)
      }
      nextRelation
    }
  }
}

class RelationStore(db: RocksDB) {
  def setRelation(relationId: Long, from: Long, to: Long, labelId: Int, category: Long, value: Map[String, Any]): Unit ={
    val keyBytes = KeyHandler.RelationKeyToBytes(relationId)
    val map = Map("from"->from, "to"->to, "labelId"->labelId, "category"->category, "prop"->value)
    val valueBytes = ByteUtils.mapToBytes(map)
    db.put(keyBytes, valueBytes)
  }

  def deleteRelation(relationId: Long): Unit ={
    val keyBytes = KeyHandler.RelationKeyToBytes(relationId)
    db.delete(keyBytes)
  }

  def getRelation(relationId: Long): StoredRelationWithProperty ={
    val keyBytes = KeyHandler.RelationKeyToBytes(relationId)
    val relation = ByteUtils.mapFromBytes(db.get(keyBytes))
    new StoredRelationWithProperty(relationId, relation("from").asInstanceOf[Long], relation("to").asInstanceOf[Long],
      relation("labelId").asInstanceOf[Int], relation("prop").asInstanceOf[Map[String, Any]],
      relation("category").asInstanceOf[Long])
  }

  def relationIsExist(relationId: Long): Boolean = {
    val keyBytes = KeyHandler.RelationKeyToBytes(relationId)
    val res = db.get(keyBytes)
    if (res != null) true else false
  }

  def getAll(): Iterator[StoredRelationWithProperty] ={
    new GetAllRocksRelation(db)
  }

  def close(): Unit ={
    db.close()
  }

  class GetAllRocksRelation(db: RocksDB) extends Iterator[StoredRelationWithProperty] {
    val keyPrefix = KeyHandler.RelationKeyPrefix()

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