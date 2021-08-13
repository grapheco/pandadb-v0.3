package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import org.rocksdb.{ReadOptions, Transaction, TransactionDB}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:14 上午 2021/8/9
 * @Modified By:
 */
class TransactionRelationLabelIndex(db: TransactionDB) {
  val readOptions = new ReadOptions()

  def set(labelId: Int, relId: Long, tx: Transaction): Unit ={
    val keyBytes = KeyConverter.toRelationTypeKey(labelId, relId)
    tx.put(keyBytes, Array.emptyByteArray)
  }

  def delete(labelId: Int, relId: Long, tx: Transaction): Unit = {
    val keyBytes = KeyConverter.toRelationTypeKey(labelId, relId)
    tx.delete(keyBytes)
  }

  def getRelations(labelId: Int, tx: Transaction): Iterator[Long] = {
    val keyPrefix = KeyConverter.toRelationTypeKey(labelId)
    val iter = tx.getIterator(readOptions)
    iter.seek(keyPrefix)

    new Iterator[Long] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(keyPrefix)

      override def next(): Long = {
        val relId: Long = ByteUtils.getLong(iter.key(), keyPrefix.length)
        iter.next()
        relId
      }
    }
  }

  def close(): Unit = db.close()

}
