package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.RocksDB

class NodeLabelNameStore(kvDB: KeyValueDB)  extends NameStore {
  override val db: KeyValueDB = kvDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.nodeLabelKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.nodeLabelKeyPrefixToBytes
  override val initInt: Int = 100000
  loadAll()
}
