package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import org.rocksdb.RocksDB

class NodeLabelNameStore(rocksDB: RocksDB)  extends NameStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.nodeLabelKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.nodeLabelKeyPrefixToBytes
  override val initInt: Int = 100000
  loadAll()
}
