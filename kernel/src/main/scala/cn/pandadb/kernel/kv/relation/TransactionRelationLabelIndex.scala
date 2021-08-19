package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import cn.pandadb.kernel.util.log.{PandaLog}
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{ReadOptions, Transaction, TransactionDB}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:14 上午 2021/8/9
 * @Modified By:
 */
class TransactionRelationLabelIndex(db: TransactionDB) {
  val readOptions = new ReadOptions()

  def set(labelId: Int, relId: Long, tx: LynxTransaction, logWriter: PandaLog): Unit ={
    val keyBytes = KeyConverter.toRelationTypeKey(labelId, relId)
    val ptx = tx.asInstanceOf[PandaTransaction]
    logWriter.writeUndoLog(ptx.id, DBNameMap.relationLabelDB, keyBytes, db.get(keyBytes))
    ptx.rocksTxMap(DBNameMap.relationLabelDB).put(keyBytes, Array.emptyByteArray)
  }

  def delete(labelId: Int, relId: Long, tx: LynxTransaction, logWriter: PandaLog): Unit = {
    val keyBytes = KeyConverter.toRelationTypeKey(labelId, relId)
    val ptx = tx.asInstanceOf[PandaTransaction]
    logWriter.writeUndoLog(ptx.id, DBNameMap.relationLabelDB, keyBytes, db.get(keyBytes))
    ptx.rocksTxMap(DBNameMap.relationLabelDB).delete(keyBytes)
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
