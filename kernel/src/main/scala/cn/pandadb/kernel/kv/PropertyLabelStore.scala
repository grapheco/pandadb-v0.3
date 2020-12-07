package cn.pandadb.kernel.kv

import org.rocksdb.RocksDB

class PropertyLabelStore(rocksDB: RocksDB) extends TokenStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.propertyNameKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.propertyNameKeyPrefixToBytes

  loadAll()

}
