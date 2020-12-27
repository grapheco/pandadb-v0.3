//import java.io.File
//
//import cn.pandadb.kernel.kv.{RocksDBGraphAPI, RocksDBStorage}
//import cn.pandadb.tools.importer.PNodeImporter
//import org.junit.{Assert, Test}
//
///**
// * @ClassName PIndexImporterTEst
// * @Description TODO
// * @Author huchuan
// * @Date 2020/12/4
// * @Version 0.1
// */
//class PIndexImporterTest {
//  val headFile = new File("F:\\graph500-22_unique_node-wrapped-head.csv")
//  val nodeFile = new File("F:\\graph500-22-node-wrapped.csv")
//  val dbPath = "F:\\PandaDB_rocksDB\\base_1B_bak"
//  val rocksDBGraphAPI = new RocksDBGraphAPI(dbPath)
//  val pNodeImporter = new PNodeImporter(dbPath, nodeFile, headFile)
//
//  @Test
//  def importNode(): Unit = {
//    val time0 = System.currentTimeMillis()
//    pNodeImporter.importNodes()
//    val time1 = System.currentTimeMillis()
//    println(s"import 240W nodes takes ${time1-time0} ms.")
//    val node = rocksDBGraphAPI.nodeAt(1)
//    Assert.assertEquals(1.toLong, node.id)
//    Assert.assertArrayEquals(Array(0), node.labelIds)
//    Assert.assertEquals(Map("id_p" -> 1.toLong, "idStr" -> "b", "flag" -> true), node.properties)
//  }
//
//  @Test
//  def createIndex(): Unit = {
//
//    // start batch
////    val time2 = System.currentTimeMillis()
//////    val indexIds = new Array[Int](10)
////    for (i <- 0 to 9) {
////      println("label "+i+" start")
////      val label = if(i==0) 9 else i-1
////
////      val indexId = rocksDBGraphAPI.createNodeIndex(label, Array[Int](6))
////      rocksDBGraphAPI.insertNodeIndexRecordsBatch(indexId, rocksDBGraphAPI.allNodes().filter(_.labelIds.contains(label)).map{
////        node=>
////          val value = node.properties("id_p").toString
////          (value, node.id)
////      })
//////      println("label "+i+" finish")
////    }
////
////    val time3 = System.currentTimeMillis()
////    println(s"create index takes ${time3-time2} ms.")
//////      rocksDBGraphAPI.dropNodeIndex(1, Array[Int](5))
//  }
//
//  @Test
//  def searchIndex(): Unit = {
//    val indexId_str = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](5))
//    val indexId_p = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](6))
//    val time0 = System.currentTimeMillis()
//    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"bbbba".getBytes()).toList)
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"eabcd".getBytes()).count(_=>true))
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"gbcde".getBytes()).count(_=>true))
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"ccbbd".getBytes()).count(_=>true))
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"ddaabb".getBytes()).count(_=>true))
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"dabefcj".getBytes()).count(_=>true))
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"ebacdfh".getBytes()).count(_=>true))
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"hjgbacaaaaaa".getBytes()).count(_=>true))
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"jjjjjjj".getBytes()).count(_=>true))
////    println(rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"ggggggg".getBytes()).count(_=>true))
//    val time1 = System.currentTimeMillis()
//    println(s"find 10 records takes ${time1-time0} ms.")
//  }
//
//  @Test
//  def searchIndexStartWith(): Unit = {
//    val indexId_str = rocksDBGraphAPI.getNodeIndexId(0,Array[Int](5))
//    val indexId_p = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](6))
//    val time0 = System.currentTimeMillis()
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"ga").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"ea").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"bb").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"cc").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"dd").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"da").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"eba").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"hjgbacaaaaaa").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"jj").count(_=>true))
//    println(rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"b").count(_=>true))
//    val time1 = System.currentTimeMillis()
//    println(s"find 10 records takes ${time1-time0} ms.")
//  }
//
//  @Test
//  def searchIndex2(): Unit = {
//    val indexId_str = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](5))
//    val indexId_p = rocksDBGraphAPI.getNodeIndexId(1,Array[Int](6))
////    println(indexId_str)
//    val time0 = System.currentTimeMillis()
//
////    val id = rocksDBGraphAPI.findStringStartWithByIndex(indexId_str,"hccccc")
//    val id = rocksDBGraphAPI.findNodeIndexRecords(indexId_str,"hccccc".getBytes())
//
//    val time2 = System.currentTimeMillis()
//    println(s"find  records takes ${time2-time0} ms.")
//
//    while (id.hasNext) {
//      println(rocksDBGraphAPI.nodeAt(id.next()).id)
//    }
//    val time1 = System.currentTimeMillis()
//    println(s" takes ${time1-time2} ms.")
//  }
//}
