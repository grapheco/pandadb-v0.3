//package cn.pandadb.kernel.kv.meta
//
//import cn.pandadb.kernel.kv.KeyConverter
//import cn.pandadb.kernel.kv.db.KeyValueDB
//import org.rocksdb.RocksDB
//
///**
// * @ClassName IndexIdGenerator
// * @Description TODO
// * @Author huchuan
// * @Date 2021/1/5
// * @Version 0.1
// */
//class IndexIdGenerator(override val db: KeyValueDB,
//                       override val keyBytes: Array[Byte] = KeyConverter.indexIdGeneratorKeyToBytes(),
//                       override val sequenceSize: Int = 100)
//  extends IdGenerator() {
//
//}
