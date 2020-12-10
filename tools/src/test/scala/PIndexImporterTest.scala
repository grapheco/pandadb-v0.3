import java.io.File

import cn.pandadb.kernel.kv.{NodeIndex, NodeStore, RocksDBGraphAPI, RocksDBStorage}
import org.junit.{Assert, Test}

/**
 * @ClassName PIndexImporterTEst
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/4
 * @Version 0.1
 */
class PIndexImporterTest {
  val headFile = new File("F:\\graph500-22_unique_node-wrapped-head.csv")
  val nodeFile = new File("F:\\graph500-22-node-wrapped.csv")
  val rocksDBGraphAPI = new RocksDBGraphAPI("F:\\PandaDB_rocksDB\\base_1B")
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

  @Test
  def createIndex(): Unit = {

    // start one record
//    val time0 = System.currentTimeMillis()
//    val indexId = rocksDBGraphAPI.createNodeIndex(1,Array[Int](5))
//    rocksDBGraphAPI.insertNodeIndexRecords(indexId, rocksDBGraphAPI.allNodes().map{
//      node=>
//        val value = node.properties("idStr").toString.getBytes()
//        (value, Array(value.length.toByte), node.id)
//    })
//    val time1 = System.currentTimeMillis()
//    println(s"create index takes ${time1-time0} ms.")

//    rocksDBGraphAPI.dropNodeIndex(1, Array[Int](5))

    // start batch
    val time2 = System.currentTimeMillis()
    val indexId2 = rocksDBGraphAPI.createNodeIndex(1,Array[Int](5))
    rocksDBGraphAPI.insertNodeIndexRecordsBatch(indexId2, rocksDBGraphAPI.allNodes().map{
      node=>
        val value = node.properties("idStr").toString.getBytes()
        (value, Array(value.length.toByte), node.id)
    })
    val time3 = System.currentTimeMillis()
    println(s"create index takes ${time3-time2} ms.")

  }

  @Test
  def searchIndex(): Unit = {
    val indexId_str = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](5))
    val indexId_p = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](6))
    val time0 = System.currentTimeMillis()
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"bbbba".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"eabcd".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"gbcde".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"ccbbd".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"ddaabb".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"dabefcj".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"ebacdfh".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"hjgbacaaaaaa".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"jjjjjjj".getBytes()).count(_=>true)
    rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"ggggggg".getBytes()).count(_=>true)
    val time1 = System.currentTimeMillis()
    println(s"find 10 records takes ${time1-time0} ms.")
  }

  @Test
  def searchIndexStartWith(): Unit = {
    val indexId_str = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](5))
    val indexId_p = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](6))
    val time0 = System.currentTimeMillis()
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"ga").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"ea").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"bb").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"cc").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"dd").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"da").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"eba").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"hjgbacaaaaaa").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"jj").count(_=>true))
    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"b").count(_=>true))
    val time1 = System.currentTimeMillis()
    println(s"find 10 records takes ${time1-time0} ms.")
  }
}
