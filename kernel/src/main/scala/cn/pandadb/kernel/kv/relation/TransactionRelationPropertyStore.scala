package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.store.{StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.rocksdb.{ReadOptions, Transaction, TransactionDB}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:14 上午 2021/8/9
 * @Modified By:
 */
class TransactionRelationPropertyStore(db: TransactionDB) {
  val readOptions = new ReadOptions()

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

  def get(relationId: Long, tx: Transaction): Option[StoredRelationWithProperty] = {
    val keyBytes = KeyConverter.toRelationKey(relationId)
    val res = tx.get(readOptions, keyBytes)
    if (res != null)
      Some(RelationSerializer.deserializeRelWithProps(res))
    else
      None
  }

  def exist(relationId: Long, tx: Transaction): Boolean = tx.get(readOptions, KeyConverter.toRelationKey(relationId)) != null

  def all(tx: Transaction): Iterator[StoredRelationWithProperty] = {
    new Iterator[StoredRelationWithProperty]{
      val iter = tx.getIterator(readOptions)
      iter.seekToFirst()

      override def hasNext: Boolean = iter.isValid

      override def next(): StoredRelationWithProperty = {
        val relation = RelationSerializer.deserializeRelWithProps(iter.value())
        iter.next()
        relation
      }
    }
  }

  def count(tx: Transaction): Long = {
    val iter = tx.getIterator(readOptions)
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
