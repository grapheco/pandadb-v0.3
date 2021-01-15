package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.RocksDB

class PropertyNameStore(kvDB: KeyValueDB) extends NameStore {
  override val db: KeyValueDB = kvDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.propertyNameKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.propertyNameKeyPrefixToBytes
  override val initInt: Int = 200000
  loadAll()

}
