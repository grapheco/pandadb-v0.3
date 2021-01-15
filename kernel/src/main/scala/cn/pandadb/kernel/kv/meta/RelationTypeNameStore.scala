package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.RocksDB

class RelationTypeNameStore(kvDB: KeyValueDB) extends NameStore {
  override val db: KeyValueDB = kvDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.relationTypeKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.relationTypeKeyPrefixToBytes
  override val initInt: Int = 300000
  loadAll()
}
