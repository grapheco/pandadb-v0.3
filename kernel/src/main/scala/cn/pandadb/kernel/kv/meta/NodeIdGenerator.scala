package cn.pandadb.kernel.kv.meta
import cn.pandadb.kernel.kv.KeyConverter
import org.rocksdb.RocksDB

class NodeIdGenerator(override val db: RocksDB,
                      override val keyBytes: Array[Byte] = KeyConverter.nodeIdGeneratorKeyToBytes(),
                      override val sequenceSize: Int = 100)
  extends IdGenerator() {

}
