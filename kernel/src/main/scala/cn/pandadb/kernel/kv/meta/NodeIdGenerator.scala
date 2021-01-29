//package cn.pandadb.kernel.kv.meta
//import cn.pandadb.kernel.kv.KeyConverter
//import cn.pandadb.kernel.kv.db.KeyValueDB
//import org.rocksdb.RocksDB
//
//class NodeIdGenerator(override val db: KeyValueDB,
//                      override val keyBytes: Array[Byte] = KeyConverter.nodeIdGeneratorKeyToBytes(),
//                      override val sequenceSize: Int = 100)
//  extends IdGenerator() {
//
//}
