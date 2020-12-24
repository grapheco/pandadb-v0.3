package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyHandler
import org.rocksdb.RocksDB

class PropertyNameStore(rocksDB: RocksDB) extends NameStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.propertyNameKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.propertyNameKeyPrefixToBytes

  loadAll()

}
