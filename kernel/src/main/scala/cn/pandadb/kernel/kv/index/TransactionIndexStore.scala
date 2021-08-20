package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import cn.pandadb.kernel.util.log.PandaLog
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{Transaction, TransactionDB, WriteBatch, WriteOptions}

class TransactionIndexStore(db: TransactionDB, logWriter: PandaLog){

  type IndexId   = Int
  type NodeId    = Long
  /**
   * Single Column Index:
   * ╔══════════════════════════════════════════╗
   * ║                   key                    ║
   * ╠═════════╦══════════╦══════════╦══════════╣
   * ║ indexId ║ typeCode ║  value   ║  nodeId  ║
   * ╚═════════╩══════════╩══════════╩══════════╝
   */
  def set(indexId: IndexId, typeCode:Byte, value: Array[Byte], nodeId: NodeId, tx: LynxTransaction): Unit = {
    val key = KeyConverter.toIndexKey(indexId, typeCode, value, nodeId)

    val ptx = tx.asInstanceOf[PandaTransaction]
    logWriter.writeUndoLog(ptx.id, DBNameMap.indexDB, key, db.get(key))

    ptx.rocksTxMap(DBNameMap.indexDB).put(key, Array.emptyByteArray)
  }

  def set(indexId: IndexId, data: Iterator[(Any, Long)], tx: LynxTransaction): Unit ={
    val ptx = tx.asInstanceOf[PandaTransaction]
    val _tx = ptx.rocksTxMap(DBNameMap.indexDB)

    val batch = new WriteBatch()
    var i = 0
    while (data.hasNext){
      val d = data.next()
      val key = KeyConverter.toIndexKey(indexId, IndexEncoder.typeCode(d._1), IndexEncoder.encode(d._1), d._2)

      logWriter.writeUndoLog(ptx.id, DBNameMap.indexDB, key, Array.emptyByteArray)
      batch.put(key, Array.emptyByteArray)

      if (i % 100000 == 0){
        logWriter.flushUndoLog()
        _tx.rebuildFromWriteBatch(batch)
        batch.clear()
      }
      i += 1
    }
    _tx.rebuildFromWriteBatch(batch)
  }

   def delete(indexId: IndexId, typeCode:Byte, value: Array[Byte], nodeId: NodeId, tx: LynxTransaction): Unit = {
     val key = KeyConverter.toIndexKey(indexId, typeCode, value, nodeId)

     val ptx = tx.asInstanceOf[PandaTransaction]
     val _tx = ptx.rocksTxMap(DBNameMap.indexDB)
     logWriter.writeUndoLog(ptx.id, DBNameMap.indexDB, key, db.get(key))

     _tx.delete(key)
  }

   def deleteRange(indexId: IndexId, tx: LynxTransaction): Unit = {
     this.synchronized{
       val ptx = tx.asInstanceOf[PandaTransaction]
       val _tx = ptx.rocksTxMap(DBNameMap.indexDB)

       val startKey = KeyConverter.toIndexKey(indexId, 0, Array.emptyByteArray, 0.toLong)
       val endKey = KeyConverter.toIndexKey(indexId, Byte.MaxValue, Array.emptyByteArray, -1.toLong)

       val iter = findByPrefix(ByteUtils.intToBytes(indexId))
       iter.foreach(key =>{
         logWriter.writeUndoLog(ptx.id, DBNameMap.indexDB, key, Array.emptyByteArray)
       })

       val batch = new WriteBatch()
       batch.deleteRange(startKey, endKey)
       _tx.rebuildFromWriteBatch(batch)
     }
  }

   def update(indexId: IndexId, typeCode:Byte, value: Array[Byte],
                             nodeId: NodeId, newTypeCode:Byte, newValue: Array[Byte], tx: LynxTransaction): Unit = {
    delete(indexId, typeCode, value,  nodeId, tx)
    set(indexId, newTypeCode, newValue, nodeId, tx)
  }

  def findByPrefix(prefix: Array[Byte]): Iterator[Array[Byte]] = {
    val iter = db.newIterator()
    iter.seek(prefix)
    new Iterator[Array[Byte]]() {
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(prefix)

      override def next(): Array[Byte] = {
        val key = iter.key()
        iter.next()
        key
      }
    }
  }
}
