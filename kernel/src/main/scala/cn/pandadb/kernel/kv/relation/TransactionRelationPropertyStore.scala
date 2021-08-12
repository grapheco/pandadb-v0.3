package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.store.{StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.rocksdb.{Transaction, TransactionDB}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:14 上午 2021/8/9
 * @Modified By:
 */
class TransactionRelationPropertyStore(db: TransactionDB) {

  def set(relation: StoredRelationWithProperty, tx: Transaction): Unit = {
    val keyBytes = KeyConverter.toRelationKey(relation.id)
    tx.put(keyBytes, RelationSerializer.serialize(relation))
  }

  def set(relation: StoredRelation, tx: Transaction): Unit = {
    val keyBytes = KeyConverter.toRelationKey(relation.id)
    tx.put(keyBytes, RelationSerializer.serialize(
      new StoredRelationWithProperty(relation.id, relation.from, relation.to, relation.typeId, Map())))
  }

  def delete(relationId: Long, tx: Transaction): Unit = tx.delete(KeyConverter.toRelationKey(relationId))

  def get(relationId: Long): Option[StoredRelationWithProperty] = {
    val keyBytes = KeyConverter.toRelationKey(relationId)
    val res = db.get(keyBytes)
    if (res != null)
      Some(RelationSerializer.deserializeRelWithProps(res))
    else
      None
  }

  def exist(relationId: Long): Boolean = db.get(KeyConverter.toRelationKey(relationId)) != null

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

  def count: Long = {
    val iter = db.newIterator()
    iter.seekToFirst()
    var count:Long = 0
    while (iter.isValid){
      count+=1
      iter.next()
    }
    count
  }

  def close(): Unit = {
    db.close()
  }
}
