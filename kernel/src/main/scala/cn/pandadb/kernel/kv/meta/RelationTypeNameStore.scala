package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import org.rocksdb.RocksDB

class RelationTypeNameStore(rocksDB: RocksDB) extends NameStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.relationTypeKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.relationTypeKeyPrefixToBytes
  override val initInt: Int = 300000
  loadAll()
}
