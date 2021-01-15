package cn.pandadb.kernel.kv.index


import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

class IndexStore(db: KeyValueDB){

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
  def set(indexId: IndexId, typeCode:Byte, value: Array[Byte], nodeId: NodeId): Unit = {
    db.put(KeyConverter.toIndexKey(indexId, typeCode, value, nodeId), Array.emptyByteArray)
  }

  def set(indexId: IndexId, data: Iterator[(Any, Long)]): Unit ={
    val writeOpt = new WriteOptions()
    val batch = new WriteBatch()
    var i = 0
    while (data.hasNext){
      val d = data.next()
      batch.put(
        KeyConverter.toIndexKey(indexId, IndexEncoder.typeCode(d._1), IndexEncoder.encode(d._1), d._2),
        Array.emptyByteArray)
      if (i % 100000 == 0){
        db.write(writeOpt, batch)
        batch.clear()
      }
      i += 1
    }
    db.write(writeOpt, batch)
  }

   def delete(indexId: IndexId, typeCode:Byte, value: Array[Byte], nodeId: NodeId): Unit = {
    db.delete(KeyConverter.toIndexKey(indexId, typeCode, value, nodeId))
  }

   def deleteRange(indexId: IndexId): Unit = {
    db.deleteRange(KeyConverter.toIndexKey(indexId, 0, Array.emptyByteArray, 0.toLong),
      KeyConverter.toIndexKey(indexId, Byte.MaxValue, Array.emptyByteArray, -1.toLong))
  }

   def update(indexId: IndexId, typeCode:Byte, value: Array[Byte],
                             nodeId: NodeId, newTypeCode:Byte, newValue: Array[Byte] ): Unit = {
    delete(indexId, typeCode, value,  nodeId)
    set(indexId, newTypeCode, newValue, nodeId)
  }

}
