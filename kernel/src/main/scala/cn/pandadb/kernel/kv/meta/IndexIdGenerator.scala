package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyHandler
import org.rocksdb.RocksDB

/**
 * @ClassName IndexIdGenerator
 * @Description TODO
 * @Author huchuan
 * @Date 2021/1/5
 * @Version 0.1
 */
class IndexIdGenerator(override val db: RocksDB,
                       override val keyBytes: Array[Byte] = KeyHandler.indexIdGeneratorKeyToBytes(),
                       override val sequenceSize: Int = 100)
  extends IdGenerator() {

}
