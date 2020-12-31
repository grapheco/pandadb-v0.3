import java.io.File

import cn.pandadb.tools.importer.PNodeImporter
import org.junit.Test

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

  @Test
  def test(): Unit = {
    val importer = new PNodeImporter(dbPath, headNodeFile, srcNodeFile)
    importer.importNodes()
  }
}
