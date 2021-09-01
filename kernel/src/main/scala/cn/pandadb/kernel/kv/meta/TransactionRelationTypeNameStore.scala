package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.util.DBNameMap
import cn.pandadb.kernel.util.log.PandaLog
import org.rocksdb.TransactionDB

class TransactionRelationTypeNameStore(tdb: TransactionDB, _logWriter: PandaLog) extends TransactionNameStore {
  override val db: TransactionDB = tdb
  override val dbName: String = DBNameMap.relationMetaDB
  override val logWriter: PandaLog = _logWriter
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.relationTypeKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.relationTypeKeyPrefixToBytes
  override val initInt: Int = 300000
  loadAll()
}
