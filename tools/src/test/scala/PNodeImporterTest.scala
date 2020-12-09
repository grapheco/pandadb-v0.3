import java.io.File

import cn.pandadb.kernel.kv.{NodeStore, RocksDBGraphAPI, RocksDBStorage}
import org.junit.{Assert, Test}
import org.rocksdb.RocksDB

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 21:31 2020/12/3
 * @Modified By:
 */
class PNodeImporterTest {
  val headFile = new File("G://dataset//nodes-1k-wrapped-head.csv")
  val nodeFile = new File("G://dataset//nodes-1k-wrapped.csv")
//  val rocksdb = RocksDBStorage.getDB("src/test/output/pnodeNodeTestDB")
  val rocksDBGraphAPI = new RocksDBGraphAPI("./src/test/resources/rocksdb")
  val pNodeImporter = new PNodeImporter(nodeFile, headFile, rocksDBGraphAPI)


  @Test
  def importNode(): Unit = {
    val time0 = System.currentTimeMillis()
    pNodeImporter.importNodes()
    val time1 = System.currentTimeMillis()
    println(s"import 240W nodes takes ${time1-time0} ms.")
    val node = rocksDBGraphAPI.nodeAt(1)
    Assert.assertEquals(1.toLong, node.id)
    Assert.assertArrayEquals(Array(0), node.labelIds)
    Assert.assertEquals(Map("id_p" -> 1.toLong, "idStr" -> "b", "flag" -> true), node.properties)
  }

}
