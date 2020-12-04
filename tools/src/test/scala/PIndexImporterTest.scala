import java.io.File

import cn.pandadb.kernel.kv.{NodeIndex, NodeStore, RocksDBStorage}
import org.junit.{Assert, Test}

/**
 * @ClassName PIndexImporterTEst
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/4
 * @Version 0.1
 */
class PIndexImporterTest {
  val headFile = new File("src/test/resources/nodeHeadFile.csv")
  val nodeFile = new File("src/test/resources/s-nodeFile.csv")
//  val nodeFile = new File("D:\\dataset\\graph500-22-node-wrapped.csv")
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

  @Test
  def createIndex(): Unit = {
    val nodeIndex = new NodeIndex(rocksdb)
    val nodeStore = new NodeStore(rocksdb)

    // start
    val time0 = System.currentTimeMillis()
    val indexId = nodeIndex.createIndex(1,Array[Int](5))
    nodeIndex.insertIndexRecord(indexId, nodeStore.all.map{
      node=>
        val value = node.properties("idStr").toString.getBytes()
        (value, Array(value.length.toByte), node.id)
    })
    val time1 = System.currentTimeMillis()
    println(s"create index takes ${time1-time0} ms.")

  }

}
