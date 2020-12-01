package cn.pandadb.kernel.kv

import cn.pandadb.kernel.store.{StoredRelationWithProperty}
import org.rocksdb.RocksDB

// inEdge index
class InEdgeRelationIndexStore(db: RocksDB){
  /**
   * Index
   * ------------------------
   *      key      |  value
   * ------------------------
   * srcId + EdgeType |  DestId
   * ------------------------
   * srcId + Category | DestId
   * ------------------------
   * EdgeType + Category: is this need?
   * ------------------------
   */
  def addSrcIdAndEdgeType(fromNode: Long, edgeType: Int, toNode: Long): Unit ={
    val key = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(fromNode, edgeType)
    val res = db.get(key)
    if (res != null) {
      var set = ByteUtils.setFromBytes(res)
      set += toNode
      db.put(key, ByteUtils.setToBytes(set))
    }
    else{
      db.put(key, ByteUtils.setToBytes(Set[Long](toNode)))
    }
  }
  def addSrcIdAndCategory(fromNode: Long, category: Long, toNode: Long): Unit ={
    val key = KeyHandler.relationNodeIdAndCategoryIndexToBytes(fromNode, category)
    val res = db.get(key)

    if (res != null) {
      var set = ByteUtils.setFromBytes(res)
      set += toNode
      db.put(key, ByteUtils.setToBytes(set))
    }
    else{
      db.put(key, ByteUtils.setToBytes(Set[Long](toNode)))
    }
  }

  def getToNodesBySrcIdAndEdgeType(srcId: Long, edgeType: Int): Set[Long] ={
    val key = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(srcId, edgeType)
    val keyBytes = db.get(key)
    if (keyBytes == null) throw new NoRelationIndexGetException
    ByteUtils.setFromBytes(keyBytes)
  }
  def getToNodesBySrcIdAndCategory(srcId: Long, category: Long): Set[Long] ={
    val key = KeyHandler.relationNodeIdAndCategoryIndexToBytes(srcId, category)
    val keyBytes = db.get(key)
    if (keyBytes == null) throw new NoRelationIndexGetException
    ByteUtils.setFromBytes(keyBytes)  }

  def deleteIndexOfSrcIdAndEdgeType2ToNode(fromNode: Long, edgeType: Int, toNode: Long): Unit ={
    val key = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(fromNode, edgeType)
    val res = db.get(key)
    if (res != null){
      var set = ByteUtils.setFromBytes(res)
      set -= toNode
      if (set.size == 0) db.delete(key)
      else db.put(key, ByteUtils.setToBytes(set))
    }
  }
  def deleteIndexOfSrcIdAndCategory2ToNode(fromNode: Long, category: Long, toNode: Long): Unit ={
    val key = KeyHandler.relationNodeIdAndCategoryIndexToBytes(fromNode, category)
    val res = db.get(key)
    if (res != null){
      var set = ByteUtils.setFromBytes(res)
      set -= toNode
      if (set.size == 0) db.delete(key)
      else db.put(key, ByteUtils.setToBytes(set))
    }
  }
}

class OutEdgeRelationIndexStore(db: RocksDB){
  /**
   * Index
   * ------------------------
   *      key      |  value
   * ------------------------
   * destId + EdgeType |  srcId
   * ------------------------
   * destId + Category | srcId
   * ------------------------
   * destType + Category: is this need?
   * ------------------------
   */
  def addDestIdAndEdgeType(toNode: Long, edgeType: Int, fromNode: Long): Unit ={
    val key = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(toNode, edgeType)
    val res = db.get(key)
    if (res != null) {
      var set = ByteUtils.setFromBytes(res)
      set += fromNode
      db.put(key, ByteUtils.setToBytes(set))
    }
    else{
      db.put(key, ByteUtils.setToBytes(Set[Long](fromNode)))
    }
  }
  def addDestAndCategory(toNode: Long, category: Long, fromNode: Long): Unit ={
    val key = KeyHandler.relationNodeIdAndCategoryIndexToBytes(toNode, category)
    val res = db.get(key)

    if (res != null) {
      var set = ByteUtils.setFromBytes(res)
      set += fromNode
      db.put(key, ByteUtils.setToBytes(set))
    }
    else{
      db.put(key, ByteUtils.setToBytes(Set[Long](fromNode)))
    }
  }

  def getFromNodesByDestIdAndEdgeType(destId: Long, edgeType: Int): Set[Long] ={
    val key = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(destId, edgeType)
    val keyBytes = db.get(key)
    if (keyBytes == null) throw new NoRelationIndexGetException
    ByteUtils.setFromBytes(keyBytes)
  }
  def getFromNodesByDestIdAndCategory(destId: Long, category: Long): Set[Long] ={
    val key = KeyHandler.relationNodeIdAndCategoryIndexToBytes(destId, category)
    val keyBytes = db.get(key)
    if (keyBytes == null) throw new NoRelationIndexGetException
    ByteUtils.setFromBytes(keyBytes)
  }

  def deleteIndexOfDestIdAndEdgeType2ToNode(toNode: Long, edgeType: Int, fromNode: Long): Unit ={
    val key = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(toNode, edgeType)
    val res = db.get(key)
    if (res != null){
      var set = ByteUtils.setFromBytes(res)
      set -= fromNode
      if (set.size == 0) db.delete(key)
      else db.put(key, ByteUtils.setToBytes(set))
    }
  }
  def deleteIndexOfDestIdAndCategory2ToNode(toNode: Long, category: Long, fromNode: Long): Unit ={
    val key = KeyHandler.relationNodeIdAndCategoryIndexToBytes(toNode, category)
    val res = db.get(key)
    if (res != null){
      var set = ByteUtils.setFromBytes(res)
      set -= fromNode
      if (set.size == 0) db.delete(key)
      else db.put(key, ByteUtils.setToBytes(set))
    }
  }

}

class RelationStore(db: RocksDB) {
  def setRelation(relationId: Long, from: Long, to: Long, labelId: Int, category: Long, value: Map[String, Any]): Unit ={
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    val map = Map("from"->from, "to"->to, "labelId"->labelId, "category"->category, "prop"->value)
    val valueBytes = ByteUtils.mapToBytes(map)
    db.put(keyBytes, valueBytes)
  }

  def deleteRelation(relationId: Long): Unit ={
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    db.delete(keyBytes)
  }

  def getRelation(relationId: Long): StoredRelationWithProperty ={
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    val res = db.get(keyBytes)
    if (res != null){
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

  def getAll(): Iterator[StoredRelationWithProperty] ={
    new GetAllRocksRelation(db)
  }

  def close(): Unit ={
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

class NoRelationGetException extends Exception{
  override def getMessage: String = "no such relation to get"
}

class NoRelationIndexGetException extends Exception{
  override def getMessage: String = "no such relation index to get"
}