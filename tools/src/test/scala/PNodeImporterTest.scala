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
  def countTest(): Unit = {
    PDBMetaData.init(dbPath)
    val nodeIter = rocksDBGraphAPI.allNodes()
    var nodeCount = 0
    while (nodeIter.hasNext) {
      nodeCount += 1
      nodeIter.next()
      if(nodeCount%10000000 == 0) println(nodeCount)
    }
    println(s"nodeCount: $nodeCount")
    var relationCount = 0
    val relIter = rocksDBGraphAPI.allRelations()
    while (relIter.hasNext) {
      relationCount += 1
      relIter.next()
      if(relationCount%10000000 == 0) println(relationCount)
    }
    println(s"relationCount: $relationCount")


    Assert.assertEquals(50000000, nodeCount)
    Assert.assertEquals(99999996, relationCount)
  }

}
