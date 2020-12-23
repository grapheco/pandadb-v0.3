package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler}
import cn.pandadb.kernel.store.StoredRelationWithProperty
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.rocksdb.RocksDB

class RelationPropertyStore(db: RocksDB) {

  def set(relation: StoredRelationWithProperty): Unit = {
    val keyBytes = KeyHandler.relationKeyToBytes(relation.id)
    db.put(keyBytes, RelationSerializer.serialize(relation))
  }

  def set(relationId: Long): Unit = {
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    db.put(keyBytes, RelationSerializer.serialize(null))
  }

  def delete(relationId: Long): Unit = db.delete(KeyHandler.relationKeyToBytes(relationId))

  def get(relationId: Long): StoredRelationWithProperty = {
    val keyBytes = KeyHandler.relationKeyToBytes(relationId)
    val res = db.get(keyBytes)
    if (res != null) RelationSerializer.deserializeRelWithProps(res)
    else null
  }

  def exist(relationId: Long): Boolean = db.get(KeyHandler.relationKeyToBytes(relationId)) != null

  def all(): Iterator[StoredRelationWithProperty] = {
    new Iterator[StoredRelationWithProperty]{
      val iter = db.newIterator()
      iter.seekToFirst()

      override def hasNext: Boolean = iter.isValid


      override def next(): StoredRelationWithProperty = {
        val relation = RelationSerializer.deserializeRelWithProps(iter.value())
        iter.next()
        relation
      }
    }
  }

  def close(): Unit = {
    db.close()
  }
}