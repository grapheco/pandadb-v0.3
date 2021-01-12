package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import org.rocksdb.RocksDB

class PropertyNameStore(rocksDB: RocksDB) extends NameStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.propertyNameKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.propertyNameKeyPrefixToBytes
  override val initInt: Int = 200000
  loadAll()

}
