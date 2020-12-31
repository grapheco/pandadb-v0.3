package cn.pandadb.kernel.kv.meta
import cn.pandadb.kernel.kv.KeyHandler
import org.rocksdb.RocksDB

class NodeIdGenerator(override val db: RocksDB,
                      override val keyBytes: Array[Byte] = KeyHandler.nodeIdGeneratorKeyToBytes(),
                      override val sequenceSize: Int = 100)
  extends IdGenerator() {

}
