package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.{Transaction, TransactionDB, WriteBatch, WriteOptions}

class TransactionIndexStore(db: TransactionDB){

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
  def set(indexId: IndexId, typeCode:Byte, value: Array[Byte], nodeId: NodeId, tx: Transaction): Unit = {
    tx.put(KeyConverter.toIndexKey(indexId, typeCode, value, nodeId), Array.emptyByteArray)
  }

  def set(indexId: IndexId, data: Iterator[(Any, Long)], tx: Transaction): Unit ={
    val batch = new WriteBatch()
    var i = 0
    while (data.hasNext){
      val d = data.next()
      batch.put(
        KeyConverter.toIndexKey(indexId, IndexEncoder.typeCode(d._1), IndexEncoder.encode(d._1), d._2),
        Array.emptyByteArray)
      if (i % 100000 == 0){
        tx.rebuildFromWriteBatch(batch)
        batch.clear()
      }
      i += 1
    }
    tx.rebuildFromWriteBatch(batch)
  }

   def delete(indexId: IndexId, typeCode:Byte, value: Array[Byte], nodeId: NodeId, tx: Transaction): Unit = {
    tx.delete(KeyConverter.toIndexKey(indexId, typeCode, value, nodeId))
  }

   def deleteRange(indexId: IndexId, tx: Transaction): Unit = {
     this.synchronized{
       val batch = new WriteBatch()
       batch.deleteRange(KeyConverter.toIndexKey(indexId, 0, Array.emptyByteArray, 0.toLong),
         KeyConverter.toIndexKey(indexId, Byte.MaxValue, Array.emptyByteArray, -1.toLong))
       tx.rebuildFromWriteBatch(batch)
     }
  }

   def update(indexId: IndexId, typeCode:Byte, value: Array[Byte],
                             nodeId: NodeId, newTypeCode:Byte, newValue: Array[Byte], tx: Transaction): Unit = {
    delete(indexId, typeCode, value,  nodeId, tx)
    set(indexId, newTypeCode, newValue, nodeId, tx)
  }

}
