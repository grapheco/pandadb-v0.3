package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyHandler
import org.rocksdb.RocksDB

class RelationIdGenerator(override val db: RocksDB,
                          override val keyBytes: Array[Byte] = KeyHandler.relationIdGeneratorKeyToBytes(),
                          override val sequenceSize: Int = 100)
  extends IdGenerator() {

}