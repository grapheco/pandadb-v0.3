package cn.pandadb.kv.performance

import java.io.File

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.kv.{ByteUtils, GraphFacade, KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.store.{StoredNode, StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.Profiler
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.junit.{Before, Test}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

class RelationPerformanceTest {


  var nodeStore: NodeStoreAPI = _
  var relationStore: RelationStoreAPI = _
  var indexStore: IndexStoreAPI = _
  val path = "F:\\PandaDB_rocksDB\\graph500"
  @Before
  def init(): Unit = {

    nodeStore = new NodeStoreAPI(path)
    relationStore = new RelationStoreAPI(path)
    indexStore = new IndexStoreAPI(path)

  }

  def evaluate(lst: ArrayBuffer[Long]): Unit = {
    lst -= lst.min
    lst -= lst.max
    println(s"${lst.length} times avg cost time: ${lst.sum / lst.length.toFloat} ms")
  }

  @Test
  def temp(): Unit ={
    val iter = Source.fromFile(new File("D:\\data\\graph500\\graph500-22-node-wrapped.csv")).getLines()
    println(iter.next())
    println(iter.next())

  }

  @Test
  def degree1Test(): Unit = {
    Array[Long](1298177, 2925502, 5282, 3583116, 3543605).foreach{
      id=>
        Profiler.timing(
          {
            //        val degree1 = graphStore.findOutEdgeRelations(3543605) // 109ms
            val degree1 = relationStore.findToNodeIds(id) // 57ms
            print(s"id ${id} degree: ${degree1.length}")
          }
        )
    }

  }

  @Test
  def degree2TestCache2(): Unit = {
    //1167ms
    Array[Long](1298177, 2925502, 5282, 3583116, 3543605).foreach {
      id=>
        Profiler.timing(
          {
            println(
              relationStore
                .findToNodeIds(id)
                .toList
                .map {
                  relationStore.findToNodeIds(_).length
                }
                .sum)
          }
        )
    }
  }


  @Test
  def degree3Test(): Unit = {
    // test 10000 times, total time: 5242 ms
    Profiler.timing()
  }

  @Test
  def degreeSearch(): Unit = {

  }

  @Test
  def getRelationValueByKeyPerformance(): Unit = {
    // 0.1885 ms
    val costTimes = new ArrayBuffer[Long](10000)
    var start: Long = 0
    for (i <- 1 to 10000) {
      val chooseId = Random.nextInt(20000000)
      start = System.currentTimeMillis()
      relationStore.getRelationById(chooseId)
      costTimes += System.currentTimeMillis() - start
    }
    evaluate(costTimes)
  }

  @Test
  def getRelationValueByKeyPerformance2(): Unit = {
    relationStore.close()
//    relationStore.allRelationsWithProperty().take(5).toList.foreach(r =>println(r.id))
//    return 0
    val db = RocksDBStorage.getDB(path+"/rels")
//    val iter = db.newIterator()
//    iter.seekToFirst()
//    for(i <- 1 to 5){
//      val value = iter.value()
//      println(value)
//      iter.next()
//    }
//    return 0
    val keys = new Array[Int](1000).map { i =>
      val id = Random.nextInt(2000000)
      //        println(id)
      KeyConverter.toRelationKey(id)
    }

//    val t1 = System.currentTimeMillis()
//    val values = keys.map {
//      key =>
//        db.get(key)
//    }
//    val t2 = System.currentTimeMillis()
//    println(s"read 10000 edges cost: ${t2 - t1} ms" )
    val t3 = System.currentTimeMillis()
    var count = 0
    val value = keys.map {
      key =>
        val v = db.get(key)
        if (v != null)
          RelationSerializer.deserializeRelWithProps(v)
        else count+=1
    }
    val t4 = System.currentTimeMillis()
    println(count)
    println(s"read and de 10000 edges cost: ${t4 - t3} ms" )
  }

  @Test
  def prefixPerformance(): Unit = {
    /*
    type + Id:                        search 100,000 times, total time: 18285 ms (hits: 200000)
    type + Id + edgeType:             search 100,000 times, total time: 18045 ms (hits: 200000)
    type + Id + edgeType + category:  search 100,000 times, total time: 18219 ms (hits: 200000)
     */
    var count = 0
    val costTimes = new ArrayBuffer[Long](100000)
    val start: Long = System.currentTimeMillis()
    for (i <- 1 to 100000) {
      val chooseId = Random.nextInt(100000000).toLong
      val idStr = chooseId.toString
      var edgeType = idStr.slice(idStr.length - 1, idStr.length).toInt - 1
      if (edgeType == -1) edgeType = 9

      //      val iter = graphStore.findOutEdgeRelations(chooseId)
      //      val iter = graphStore.findOutEdgeRelations(chooseId,edgeType)
      val iter = relationStore.findOutRelations(chooseId, Some(edgeType))

      while (iter.hasNext) {
        val res = iter.next()
        //        println(res.from, res.to, res.labelId, res.properties, res.category, res.id)
        count += 1
      }
    }
    //    evaluate(costTimes)
    println(System.currentTimeMillis() - start, "ms")
    println(count)
  }

  @Test
  def getAllRelation_Performance(): Unit = {
    // 0.2 billion relation, 4201,849 ms（ 1h10min ）
    var count = 0
    val totalRelationship = 200000000
    val start = System.currentTimeMillis()
    var oneTime = System.currentTimeMillis()
    val iter = relationStore.allRelations()
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
///////////
  @Test
  def getNodesByLabelIdTest(): Unit = {
    // 10 million nodes, total time: 2,730 ms
    var count = 0
    val start = System.currentTimeMillis()
    var oneTime = System.currentTimeMillis()
    val iter = nodeStore.getNodeIdsByLabel(8)
    //    while (iter.hasNext && count < 10) { // get 1 node only need 0.2 ms
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
    // 10 million nodes, total time: 335,421 ms
//    val scanImpl = new PandaPropertyGraphScanImpl(nodeLabelStore, relLabelStore, propNameStore,
//      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
//      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
//      graphStore)
//
//    val iter = scanImpl.allNodes(Set("label0"), true)
//    var count = 0
//    var oneTime = System.currentTimeMillis()
//    val start = System.currentTimeMillis()
//    val iterator = iter.toStream.iterator
//    while (iterator.hasNext) {
//      val res = iterator.next()
//      res.properties
//      count += 1
//      if (count % 1000000 == 0) {
//        println(s"epoch: ${count / 1000000}, read 100w cost : ${System.currentTimeMillis() - oneTime} ms")
//        oneTime = System.currentTimeMillis()
//      }
//    }
//    println(System.currentTimeMillis() - start)
  }

  @Test
  def getAllNodesTest(): Unit = {
    //  0.1 billion nodes, total time: 27min (1614,483 ms)
//    val scanImpl = new PandaPropertyGraphScanImpl(nodeLabelStore, relLabelStore, propNameStore,
//      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
//      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
//      graphStore)
//
//    val iter = scanImpl.allNodes()
//    var count = 0
//    var oneTime = System.currentTimeMillis()
//    val start = System.currentTimeMillis()
//    val iterator = iter.toStream.iterator
//    while (iterator.hasNext) {
//      iterator.next()
//      count += 1
//      if (count % 1000000 == 0) {
//        println(s"epoch: ${count / 1000000}, read 100w cost : ${System.currentTimeMillis() - oneTime} ms")
//        oneTime = System.currentTimeMillis()
//      }
//    }
//    println(System.currentTimeMillis() - start)
  }

  @Test
  def tmp(): Unit = {
//    val iter = graphStore.findNodes(9)
//    val start = System.currentTimeMillis()
//    for (i <- 1 to 100000) {
//      val nodeId = iter.next()
//      mapNode(graphStore.nodeAt(nodeId))
//    }
//    println(System.currentTimeMillis() - start)
//
//
//    //    nodes.map(nodeId => mapNode(graphStore.nodeAt(nodeId))).toIterable
//
//    def mapNode(node: StoredNode): Node[Long] = {
//      new Node[Long] {
//        override type I = this.type
//
//        override def id: Long = node.id
//
//        override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeLabelStore.key(id).get)
//
//        override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???
//
//        override def properties: CypherMap = {
//          var props: Map[String, Any] = null
//          if (node.isInstanceOf[StoredNodeWithProperty_tobe_deprecated]) {
//            props = node.asInstanceOf[StoredNodeWithProperty_tobe_deprecated].properties
//          }
//          else {
//            val n = graphStore.nodeAt(node.id)
//            props = n.asInstanceOf[StoredNodeWithProperty_tobe_deprecated].properties
//          }
//          CypherMap(props.toSeq: _*)
//        }
//      }
//    }
  }

  @Test
  def serializableTest(): Unit = {
//    val data1 = Map("id_p" -> 1L, "idStr" -> "b", "flag" -> true) // 1.1 ms
//    val data2 = Map("id_p" -> 10L, "idStr" -> "bb", "flag" -> true) // 1.1ms
//    val data3 = Map("id_p" -> 100L, "idStr" -> "bbb", "flag" -> true) // 1.2ms
//    val data4 = Map("id_p" -> 1000L, "idStr" -> "bbbb", "flag" -> true) // 1.2ms
//    val data5 = Map("id_p" -> 10000L, "idStr" -> "bbbbb", "flag" -> true) // 1.4ms
//    val data6 = Map("id_p" -> 100000L, "idStr" -> "bbbbbb", "flag" -> true) //1.1 ms
//    val data7 = Map("id_p" -> 1000000L, "idStr" -> "bbbbbbbb", "flag" -> true) // 1.1ms
//
//    val start = System.currentTimeMillis()
//    for (i <- 1 to 10) {
//      ByteUtils.mapToBytes(data7)
//    }
//    println((System.currentTimeMillis() - start) / 10.0)
  }

  @Test
  def deserializableTest(): Unit = {
//    val data1 = ByteUtils.mapToBytes(Map("id_p" -> 1L, "idStr" -> "b", "flag" -> true)) // 1.0 ms
//    val data2 = ByteUtils.mapToBytes(Map("id_p" -> 10L, "idStr" -> "bb", "flag" -> true)) // 0.9 ms
//    val data3 = ByteUtils.mapToBytes(Map("id_p" -> 100L, "idStr" -> "bbb", "flag" -> true)) // 0.8 ms
//    val data4 = ByteUtils.mapToBytes(Map("id_p" -> 1000L, "idStr" -> "bbbb", "flag" -> true)) // 0.8 ms
//    val data5 = ByteUtils.mapToBytes(Map("id_p" -> 10000L, "idStr" -> "bbbbb", "flag" -> true)) // 0.9 ms
//    val data6 = ByteUtils.mapToBytes(Map("id_p" -> 100000L, "idStr" -> "bbbbbb", "flag" -> true)) // 1.0 ms
//    val data7 = ByteUtils.mapToBytes(Map("id_p" -> 1000000L, "idStr" -> "bbbbbbbb", "flag" -> true)) // 0.9 ms
//
//    val start = System.currentTimeMillis()
//    for (i <- 1 to 100000) {
//      ByteUtils.mapFromBytes(data1)
//    }
//    println((System.currentTimeMillis() - start) / 100000.0)
  }
}
