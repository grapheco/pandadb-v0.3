//package cn.pandadb.kv.performance
//
//import java.io.File
//
//import cn.pandadb.kernel.kv.index.IndexStoreAPI
//import cn.pandadb.kernel.kv.node.NodeStoreAPI
//import cn.pandadb.kernel.kv.relation.RelationStoreAPI
//import cn.pandadb.kernel.kv.{ByteUtils, GraphFacade, KeyConverter, RocksDBStorage}
//import cn.pandadb.kernel.store.{StoredNode, StoredRelation, StoredRelationWithProperty}
//import cn.pandadb.kernel.util.Profiler
//import cn.pandadb.kernel.util.serializer.RelationSerializer
//import org.junit.{Before, Test}
//
//import scala.collection.mutable.ArrayBuffer
//import scala.io.Source
//import scala.util.Random
//
//class RelationPerformanceTest {
//
//  var nodeStore: NodeStoreAPI = _
//  var relationStore: RelationStoreAPI = _
//  var indexStore: IndexStoreAPI = _
//  val path = "F:\\PandaDB_rocksDB\\graph500"
//
//  @Before
//  def init(): Unit = {
//
//    nodeStore = new NodeStoreAPI(path)
//    relationStore = new RelationStoreAPI(path)
//    indexStore = new IndexStoreAPI(path)
//
//  }
//
//  def evaluate(lst: ArrayBuffer[Long]): Unit = {
//    lst -= lst.min
//    lst -= lst.max
//    println(s"${lst.length} times avg cost time: ${lst.sum / lst.length.toFloat} ms")
//  }
//
//  @Test
//  def degree1Test(): Unit = {
//    Array[Long](1298177, 2925502, 5282, 3583116, 3543605).foreach {
//      id =>
//        Profiler.timing(
//          {
//            //        val degree1 = graphStore.findOutEdgeRelations(3543605) // 109ms
//            val degree1 = relationStore.findToNodeIds(id) // 57ms
//            print(s"id ${id} degree: ${degree1.length}")
//          }
//        )
//    }
//
//  }
//
//  @Test
//  def degree2TestCache2(): Unit = {
//    //1167ms
//    Array[Long](1298177, 2925502, 5282, 3583116, 3543605).foreach {
//      id =>
//        Profiler.timing(
//          {
//            println(
//              relationStore
//                .findToNodeIds(id)
//                .toList
//                .map {
//                  relationStore.findToNodeIds(_).length
//                }
//                .sum)
//          }
//        )
//    }
//  }
//
//  @Test
//  def getRelationValueByKeyPerformance(): Unit = {
//    // 0.1885 ms
//    val costTimes = new ArrayBuffer[Long](10000)
//    var start: Long = 0
//    for (i <- 1 to 10000) {
//      val chooseId = Random.nextInt(20000000)
//      start = System.currentTimeMillis()
//      relationStore.getRelationById(chooseId)
//      costTimes += System.currentTimeMillis() - start
//    }
//    evaluate(costTimes)
//  }
//
//  @Test
//  def getRelationValueByKeyPerformance2(): Unit = {
//    relationStore.close()
//    val db = RocksDBStorage.getDB(path + "/rels")
//    val keys = new Array[Int](1000).map { i =>
//      val id = Random.nextInt(2000000)
//      KeyConverter.toRelationKey(id)
//    }
//
//    val t3 = System.currentTimeMillis()
//    var count = 0
//    val value = keys.map {
//      key =>
//        val v = db.get(key)
//        if (v != null)
//          RelationSerializer.deserializeRelWithProps(v)
//        else count += 1
//    }
//    val t4 = System.currentTimeMillis()
//    println(count)
//    println(s"read and de 10000 edges cost: ${t4 - t3} ms")
//  }
//
//  @Test
//  def prefixPerformance(): Unit = {
//    /*
//    type + Id:                        search 100,000 times, total time: 18285 ms (hits: 200000)
//    type + Id + edgeType:             search 100,000 times, total time: 18045 ms (hits: 200000)
//    type + Id + edgeType + category:  search 100,000 times, total time: 18219 ms (hits: 200000)
//     */
//    var count = 0
//    val costTimes = new ArrayBuffer[Long](100000)
//    val start: Long = System.currentTimeMillis()
//    for (i <- 1 to 100000) {
//      val chooseId = Random.nextInt(100000000).toLong
//      val idStr = chooseId.toString
//      var edgeType = idStr.slice(idStr.length - 1, idStr.length).toInt - 1
//      if (edgeType == -1) edgeType = 9
//      val iter = relationStore.findOutRelations(chooseId, Some(edgeType))
//
//      while (iter.hasNext) {
//        val res = iter.next()
//        count += 1
//      }
//    }
//    println(System.currentTimeMillis() - start, "ms")
//    println(count)
//  }
//
//  @Test
//  def getAllRelation_Performance(): Unit = {
//    // 0.2 billion relation, 4201,849 ms（ 1h10min ）
//    var count = 0
//    val totalRelationship = 200000000
//    val start = System.currentTimeMillis()
//    var oneTime = System.currentTimeMillis()
//    val iter = relationStore.allRelations()
//    while (iter.hasNext) {
//      iter.next()
//      count += 1
//      if (count % 1000000 == 0) {
//        println(s"epoch: ${count / 1000000} / ${totalRelationship / 1000000} : ${System.currentTimeMillis() - oneTime} ms")
//        oneTime = System.currentTimeMillis()
//      }
//    }
//    println(s"cost time: ${System.currentTimeMillis() - start} ms")
//  }
//
//  @Test
//  def getNodesByLabelIdTest(): Unit = {
//    // 10 million nodes, total time: 2,730 ms
//    var count = 0
//    val start = System.currentTimeMillis()
//    var oneTime = System.currentTimeMillis()
//    val iter = nodeStore.getNodeIdsByLabel(8)
//    while (iter.hasNext) {
//      iter.next()
//      count += 1
//      if (count % 1000000 == 0) {
//        println(s"epoch: ${count / 1000000}, read 100w cost : ${System.currentTimeMillis() - oneTime} ms")
//        oneTime = System.currentTimeMillis()
//      }
//    }
//    println(s"cost time: ${System.currentTimeMillis() - start} ms")
//  }
//}
