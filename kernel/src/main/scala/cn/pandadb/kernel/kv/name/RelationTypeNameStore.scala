package cn.pandadb.kernel.kv.name

import cn.pandadb.kernel.kv.KeyHandler
import org.rocksdb.RocksDB

class RelationTypeNameStore(rocksDB: RocksDB) extends NameStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.relationLabelKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.relationLabelKeyPrefixToBytes

  loadAll()
}
