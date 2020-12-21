import java.io.File

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{RocksDBGraphAPI, RocksDBStorage}
import cn.pandadb.tools.importer.PRelationImporter
import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:19 2020/12/4
 * @Modified By:
 */
class PRelationImporterTest {
  val headFile = new File("src/test/resources/edgeHeadFile.csv")
  val edgeFile = new File("src/test/resources/s-edgeFile.csv")
//  val rocksdb = RocksDBStorage.getDB("src/test/output/pnodeEdgeNodeTestDB")
  val rocksDBGraphAPI = new RocksDBGraphAPI("./src/test/resources/rocksdb")
  val pEdgeImporter = new PRelationImporter(edgeFile, headFile, rocksDBGraphAPI)

  @Test
  def importEdges(): Unit = {
    val time0 = System.currentTimeMillis()
    pEdgeImporter.importEdges()
    val time1 = System.currentTimeMillis()

    val relation = rocksDBGraphAPI.relationAt(2)
    Assert.assertEquals(2, relation.id)
    Assert.assertEquals(2303395, relation.from)
    Assert.assertEquals(1298177, relation.to)
    Assert.assertEquals(PDBMetaData.getTypeId("type5"), relation.typeId)
    Assert.assertEquals(300, relation.category)

  }

}
