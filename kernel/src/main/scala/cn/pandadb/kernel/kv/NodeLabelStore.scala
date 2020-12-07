package cn.pandadb.kernel.kv
import org.rocksdb.RocksDB

class NodeLabelStore(rocksDB: RocksDB)  extends TokenStore {
  override val db: RocksDB = rocksDB
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.nodeLabelKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.nodeLabelKeyPrefixToBytes

  loadAll()
}
