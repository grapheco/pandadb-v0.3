package cn.pandadb.kv.performance

import java.io.File

import cn.pandadb.kernel.kv.{ByteUtils, GraphFacadeWithPPD, NodeLabelStore, PandaPropertyGraphScanImpl, PropertyNameStore, RelationLabelStore, RocksDBGraphAPI, TokenStore}
import cn.pandadb.kernel.store.FileBasedIdGen
import org.junit.{Before, Test}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class RelationPerformanceTest {
  var nodeLabelStore: TokenStore = _
  var relLabelStore: TokenStore = _
  var propNameStore: TokenStore = _
  var graphStore: RocksDBGraphAPI = _

  var graphFacade: GraphFacadeWithPPD = _

  @Before
  def init(): Unit = {
    //    FileUtils.deleteDirectory(new File("./testdata/output"))
    //    new File("./testdata/output").mkdirs()
    //    new File("./testdata/output/nodelabels").createNewFile()
    //    new File("./testdata/output/rellabels").createNewFile()

    graphStore = new RocksDBGraphAPI("D:\\data\\rocksdb")

    nodeLabelStore = new NodeLabelStore(graphStore.getRocksDB)
    relLabelStore = new RelationLabelStore(graphStore.getRocksDB)
    propNameStore = new PropertyNameStore(graphStore.getRocksDB)

    graphFacade = new GraphFacadeWithPPD(nodeLabelStore, relLabelStore, propNameStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      graphStore,
      {}
    )
  }

  def evaluate(lst: ArrayBuffer[Long]): Unit = {
    lst -= lst.min
    lst -= lst.max
    println(s"${lst.length} times avg cost time: ${lst.sum / lst.length.toFloat} ms")
  }

  @Test
  def getRelationValueByKeyPerformance(): Unit = {
    // 0.1591 ms
    val costTimes = new ArrayBuffer[Long](10000)
    var start: Long = 0
    for (i <- 1 to 10002) {
      val chooseId = Random.nextInt(200000000)
      start = System.currentTimeMillis()
      graphStore.relationAt(chooseId)
      costTimes += System.currentTimeMillis() - start
    }
    evaluate(costTimes)
  }

  @Test
  def prefixPerformance(): Unit = {
    val costTimes = new ArrayBuffer[Long](10000)
    var start: Long = 0
    for (i <- 1 to 10002) {
      val chooseId = Random.nextInt(100000000)
      val edgeType = Random.nextInt(10)
      val category = Random.nextInt(10)

      start = System.currentTimeMillis()
      //      val iter = graphStore.findOutEdgeRelations(chooseId)
      //      val iter = graphStore.findOutEdgeRelations(chooseId,edgeType)
      val iter = graphStore.findOutEdgeRelations(chooseId, edgeType, category)

      while (iter.hasNext) {
        iter.next()
      }
      costTimes += System.currentTimeMillis() - start
    }
    evaluate(costTimes)
  }

  @Test
  def getAllRelation_Performance(): Unit = {
    var count = 0
    val totalRelationship = 200000000
    val start = System.currentTimeMillis()
    var oneTime = System.currentTimeMillis()
    val iter = graphStore.allRelations()

    while (iter.hasNext) {
      iter.next()
      count += 1
      if (count % 1000000 == 0) {
        println(s"epoch: ${count / 1000000} / ${totalRelationship / 1000000} : ${System.currentTimeMillis() - oneTime} ms")
        oneTime = System.currentTimeMillis()
      }
    }
    println(s"cost time: ${System.currentTimeMillis() - start} ms")
  }

  @Test
  def getNodesByLabelIdTest(): Unit = {
    // 2730 ms, avg 100w: 230 ms
    var count = 0
    val start = System.currentTimeMillis()
    var oneTime = System.currentTimeMillis()
    val iter = graphStore.findNodes(9)
    while (iter.hasNext) {
      iter.next()
      count += 1
      if (count % 1000000 == 0) {
        println(s"epoch: ${count / 1000000}, read 100w cost : ${System.currentTimeMillis() - oneTime} ms")
        oneTime = System.currentTimeMillis()
      }
    }
    println(s"cost time: ${System.currentTimeMillis() - start} ms")
  }

  @Test
  def getNodesByLabelNameTest(): Unit = {
    // 335421 ms, 100w avg: 33000 ms
    val scanImpl = new PandaPropertyGraphScanImpl(nodeLabelStore, relLabelStore, propNameStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      graphStore)

    val iter = scanImpl.allNodes(Set("label0"), true)
    var count = 0
    var oneTime = System.currentTimeMillis()
    val start = System.currentTimeMillis()
    val iterator = iter.toStream.iterator
    while (iterator.hasNext) {
      iterator.next()
      count += 1
      if (count % 1000000 == 0) {
        println(s"epoch: ${count / 1000000}, read 100w cost : ${System.currentTimeMillis() - oneTime} ms")
        oneTime = System.currentTimeMillis()
      }
    }
    println(System.currentTimeMillis() - start)
  }

  @Test
  def getAllNodesTest(): Unit = {
    //  26min56s (1614483 ms)
    val scanImpl = new PandaPropertyGraphScanImpl(nodeLabelStore, relLabelStore, propNameStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      graphStore)

    val iter = scanImpl.allNodes()
    var count = 0
    var oneTime = System.currentTimeMillis()
    val start = System.currentTimeMillis()
    val iterator = iter.toStream.iterator
    while (iterator.hasNext) {
      iterator.next()
      count += 1
      if (count % 1000000 == 0) {
        println(s"epoch: ${count / 1000000}, read 100w cost : ${System.currentTimeMillis() - oneTime} ms")
        oneTime = System.currentTimeMillis()
      }
    }
    println(System.currentTimeMillis() - start)
  }

  @Test
  def serializableTest(): Unit ={
    val data1 = Map("id_p"->1L, "idStr"->"b", "flag"->true) // 1.1 ms
    val data2 = Map("id_p"->10L, "idStr"->"bb", "flag"->true) // 1.1ms
    val data3 = Map("id_p"->100L, "idStr"->"bbb", "flag"->true) // 1.2ms
    val data4 = Map("id_p"->1000L, "idStr"->"bbbb", "flag"->true) // 1.2ms
    val data5 = Map("id_p"->10000L, "idStr"->"bbbbb", "flag"->true) // 1.4ms
    val data6 = Map("id_p"->100000L, "idStr"->"bbbbbb", "flag"->true) //1.1 ms
    val data7 = Map("id_p"->1000000L, "idStr"->"bbbbbbbb", "flag"->true)// 1.1ms

    val start = System.currentTimeMillis()
    for (i <- 1 to 10){
      ByteUtils.mapToBytes(data7)
    }
    println((System.currentTimeMillis() - start) / 10.0)
  }

  @Test
  def deserializableTest(): Unit ={
    val data1 = ByteUtils.mapToBytes(Map("id_p"->1L, "idStr"->"b", "flag"->true)) // 1.0 ms
    val data2 = ByteUtils.mapToBytes(Map("id_p"->10L, "idStr"->"bb", "flag"->true)) // 0.9 ms
    val data3 = ByteUtils.mapToBytes(Map("id_p"->100L, "idStr"->"bbb", "flag"->true)) // 0.8 ms
    val data4 = ByteUtils.mapToBytes(Map("id_p"->1000L, "idStr"->"bbbb", "flag"->true)) // 0.8 ms
    val data5 = ByteUtils.mapToBytes(Map("id_p"->10000L, "idStr"->"bbbbb", "flag"->true)) // 0.9 ms
    val data6 = ByteUtils.mapToBytes(Map("id_p"->100000L, "idStr"->"bbbbbb", "flag"->true)) // 1.0 ms
    val data7 = ByteUtils.mapToBytes(Map("id_p"->1000000L, "idStr"->"bbbbbbbb", "flag"->true)) // 0.9 ms

    val start = System.currentTimeMillis()
    for (i <- 1 to 10){
      ByteUtils.mapFromBytes(data1)
    }
    println((System.currentTimeMillis() - start) / 10.0)
  }
}
