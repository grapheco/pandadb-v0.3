package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.store.{StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import cn.pandadb.kernel.util.log.PandaLog
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{ReadOptions, Transaction, TransactionDB}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:14 上午 2021/8/9
 * @Modified By:
 */
class TransactionRelationPropertyStore(db: TransactionDB) {
  val readOptions = new ReadOptions()

  def set(relation: StoredRelationWithProperty, tx: LynxTransaction, logWriter: PandaLog): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]
    val keyBytes = KeyConverter.toRelationKey(relation.id)
    logWriter.writeUndoLog(ptx.id, DBNameMap.relationDB, keyBytes, db.get(keyBytes))
    ptx.rocksTxMap(DBNameMap.relationDB).put(keyBytes, RelationSerializer.serialize(relation))
  }

  def set(relation: StoredRelation, tx: LynxTransaction, logWriter: PandaLog): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]
    val keyBytes = KeyConverter.toRelationKey(relation.id)
    logWriter.writeUndoLog(ptx.id, DBNameMap.relationDB, keyBytes, db.get(keyBytes))
    ptx.rocksTxMap(DBNameMap.relationDB).put(keyBytes, RelationSerializer.serialize(
      new StoredRelationWithProperty(relation.id, relation.from, relation.to, relation.typeId, Map())))
  }

  def delete(relationId: Long, tx: LynxTransaction, logWriter: PandaLog): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]
    val keyBytes = KeyConverter.toRelationKey(relationId)
    logWriter.writeUndoLog(ptx.id, DBNameMap.relationDB, keyBytes, db.get(keyBytes))
    ptx.rocksTxMap(DBNameMap.relationDB).delete(keyBytes)
  }

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
