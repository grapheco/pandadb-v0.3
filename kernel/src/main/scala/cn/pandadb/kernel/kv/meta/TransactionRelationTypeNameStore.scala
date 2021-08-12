package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.TransactionDB

class TransactionRelationTypeNameStore(tdb: TransactionDB) extends TransactionNameStore {
  override val db: TransactionDB = tdb
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.relationTypeKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.relationTypeKeyPrefixToBytes
  override val initInt: Int = 300000
  loadAll()
}
