import java.io.File

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.tools.importer.PRelationImporter
import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:19 2020/12/4
 * @Modified By:
 */
class PRelationImporterTest {
  @Test
  def importEdges(): Unit = {
    val dbPath = "F:\\PandaDB_rocksDB\\10kw"
    PDBMetaData.init(dbPath)
    PDBMetaData.persist(dbPath)
  }

}
