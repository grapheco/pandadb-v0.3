import java.io.File

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{RocksDBGraphAPI, RocksDBStorage}
import cn.pandadb.tools.importer.PEdgeImporter
import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:19 2020/12/4
 * @Modified By:
 */
class PEdgeImporterTest {
  val headFile = new File("src/test/resources/edgeHeadFile.csv")
  val edgeFile = new File("D:\\dataset\\graph500-22-wrapped.csv")
//  val rocksdb = RocksDBStorage.getDB("src/test/output/pnodeEdgeNodeTestDB")
  val rocksDBGraphAPI = new RocksDBGraphAPI("./src/test/resources/rocksdb")
  val pEdgeImporter = new PEdgeImporter(edgeFile, headFile, rocksDBGraphAPI)

  @Test
  def importEdges(): Unit = {
    val time0 = System.currentTimeMillis()
    pEdgeImporter.importEdges()
    val time1 = System.currentTimeMillis()
    println(s"import 6kw edges takes ${time1 - time0} ms.")
//    val relStore = new RelationStore(rocksdb)
//    val edge = relStore.getRelation(2)
    val edge = rocksDBGraphAPI.relationAt(2)
    Assert.assertEquals(2, edge.id)
    Assert.assertEquals(2303395, edge.from)
    Assert.assertEquals(1298177, edge.to)
    Assert.assertEquals(PDBMetaData.getTypeId("type5"), edge.typeId)
//    Assert.assertEquals(Map("fromId_num" -> 2303395, "toId_str" -> "1298177", "weight" -> 5), edge.properties)
  }

}
