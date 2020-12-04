import java.io.File

import cn.pandadb.kernel.kv.{NodeStore, RocksDBStorage}
import org.junit.{Assert, Test}
import org.rocksdb.RocksDB

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 21:31 2020/12/3
 * @Modified By:
 */
class PNodeImporterTest {
  val headFile = new File("src/test/resources/nodeHeadFile.csv")
  val nodeFile = new File("D:\\dataset\\graph500-22-node-wrapped.csv")
  val rocksdb = RocksDBStorage.getDB("src/test/output/pnodeNodeTestDB")
  val pNodeImporter = new PNodeImporter(nodeFile, rocksdb, headFile)

  @Test
  def importNode(): Unit = {
    val time0 = System.currentTimeMillis()
    pNodeImporter.importNodes()
    val time1 = System.currentTimeMillis()
    println(s"import 240W nodes takes ${time1-time0} ms.")
    val nodeStore = new NodeStore(rocksdb)
    val node = nodeStore.get(1813314)
    Assert.assertEquals(1813314.toLong, node.id)
    Assert.assertArrayEquals(Array(1), node.labelIds)
    Assert.assertEquals(Map("id_p" -> 1813314.toLong, "idStr" -> "bibddbe", "flag" -> false), node.properties)
  }

}
