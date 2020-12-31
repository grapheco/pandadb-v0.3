package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyHandler
import org.rocksdb.RocksDB

class RelationTypeNameStore(rocksDB: RocksDB) extends NameStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.relationTypeKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.relationTypeKeyPrefixToBytes
  override val initInt: Int = 300000
  loadAll()
}
