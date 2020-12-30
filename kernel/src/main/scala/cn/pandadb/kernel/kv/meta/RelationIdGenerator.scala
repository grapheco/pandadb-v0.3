package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyHandler
import org.rocksdb.RocksDB

class RelationIdGenerator(db: RocksDB,
                      keyBytes: Array[Byte] = KeyHandler.nodeIdGeneratorKeyToBytes(),
                      sequenceSize: Int = 100)
  extends IdGenerator(db, keyBytes, sequenceSize) {

}
