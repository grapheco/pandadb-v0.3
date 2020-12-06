package cn.pandadb.kernel.kv

import org.rocksdb.RocksDB

class RelationLabelStore(rocksDB: RocksDB) extends TokenStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.relationLabelKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.relationLabelKeyPrefixToBytes

  loadAll()
}
