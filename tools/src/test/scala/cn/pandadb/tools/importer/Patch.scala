//import java.io.File
//
//import cn.pandadb.kernel.PDBMetaData
//import cn.pandadb.kernel.PDBMetaData.{_nodeIdAllocator, _relationIdAllocator}
//import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter, RocksDBStorage}
//import cn.pandadb.tools.importer.PRelationImporter
//import org.junit.{Assert, Test}
//import org.rocksdb.FlushOptions
//
///**
// * @Author: Airzihao
// * @Description:
// * @Date: Created at 15:19 2020/12/4
// * @Modified By:
// */
//class PRelationImporterTest {
//  @Test
//  def importEdges(): Unit = {
//    val dbPath = "F:\\PandaDB_rocksDB\\10kw"
//    PDBMetaData.init(dbPath)
//    PDBMetaData.persist(dbPath)
//    val nodeMetaDB = RocksDBStorage.getDB(s"${dbPath}/nodeMeta")
//    val relMetaDB = RocksDBStorage.getDB(s"${dbPath}/relationMeta")
//    nodeMetaDB.put(KeyConverter.nodeIdGeneratorKeyToBytes(), ByteUtils.longToBytes(100000000))
//    relMetaDB.put(KeyConverter.relationIdGeneratorKeyToBytes(), ByteUtils.longToBytes(200000000 - 4))
//    nodeMetaDB.flush(new FlushOptions)
//    relMetaDB.flush(new FlushOptions)
//    nodeMetaDB.close()
//    relMetaDB.close()
//  }
//
//}
