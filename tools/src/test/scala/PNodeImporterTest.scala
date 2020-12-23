import java.io.File

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{RocksDBGraphAPI, RocksDBStorage}
import cn.pandadb.tools.importer.PNodeImporter
import org.junit.{Assert, Test}
import org.rocksdb.RocksDB

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 21:31 2020/12/3
 * @Modified By:
 */
class PNodeImporterTest {
  val srcNodeFile = new File("D://GitSpace//ScalaUtils//nodes500.csv")
  val srcEdgeFile = new File("D://GitSpace//ScalaUtils//edges500.csv")
  val headNodeFile = new File("D://GitSpace//ScalaUtils//nodeHead.csv")
  val headEdgeFile = new File("D://GitSpace//ScalaUtils//relationHead.csv")
  val dbPath = "C:\\PandaDB\\base_50M"
  val rocksDBGraphAPI = new RocksDBGraphAPI(dbPath)


  @Test
  def correctTest(): Unit = {
    PDBMetaData.init(dbPath)
    val iter = rocksDBGraphAPI.nodeAt(1)
    iter
  }

}
