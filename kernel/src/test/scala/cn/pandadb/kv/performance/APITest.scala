package cn.pandadb.kv.performance

import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.grapheco.lynx.{LynxId, LynxNode, LynxValue}
import org.junit.{Before, Test}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

import scala.collection.mutable.ArrayBuffer

/**
 * @ClassName APITest
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/31
 * @Version 0.1
 */
class APITest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacadeWithPPD = _


  @Before
  def setup(): Unit = {

    val dbPath = "F:\\graph500"
    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacadeWithPPD(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  @Test
  def api(): Unit = {
    Profiler.timing({
      println("Test preheat")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false) {
          res += node
          count += 1
        }
      }
    })

    Profiler.timing({
      println("Test match (n:label0) where n.idStr='ha' return n")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("idStr")
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == "ha") {
          res += node
        }
      }
    })

    Profiler.timing({
      println("Test match (n:label0) where n.flag = false return n limit 10")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false) {
          res += node
          count += 1
        }
      }
    })

    Profiler.timing({
      println("Test match (n:label0) where n.flag = false and n.id_p=70 return n limit 10")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      val prop2 = nodeStore.getPropertyKeyId("id_p")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false &&
          node.properties.getOrElse(prop2, null) == 70) {
          res += node
          count += 1
        }
      }
    })

    Profiler.timing({
      println("Test match (n:label0) where n.flag = false and n.idStr='ha' return n limit 10")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      val prop2 = nodeStore.getPropertyKeyId("idStr")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false &&
          node.properties.getOrElse(prop2, null) == "ha") {
          res += node
          count += 1
        }
      }
    })

    Profiler.timing({
      println("Test match (n:label0) where n.flag = false and n.idStr='ea' and n.id_p=40 return n limit 10")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      val prop2 = nodeStore.getPropertyKeyId("idStr")
      val prop3 = nodeStore.getPropertyKeyId("id_p")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false &&
          node.properties.getOrElse(prop2, null) == "ea" &&
          node.properties.getOrElse(prop3, null) == 40) {
          res += node
          count += 1
        }
      }
    })

    Profiler.timing({
      println("Test match (n:label0) where n.flag = false and n.name='Alice Panda' count(n)")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      val prop2 = nodeStore.getPropertyKeyId("name")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
//      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false &&
          node.properties.getOrElse(prop2, null) == "Alice Panda") {
//          res += node
          count += 1
        }
      }
      println("count: "+count)
    })
  }

  @Test
  def cypher(): Unit ={
        val res5 = graphFacade.cypher("match (n:label0) where n.idStr='ha' return n")
    Profiler.timing({
    val res7 = graphFacade.cypher("match (n:label0) where n.idStr='ha' return n")
    res7.show()})

//      val res = graphFacade.cypher("match (n:label1) where n.idStr='b' return n")
//    Profiler.timing({
//      val res2 = graphFacade.cypher("match (n:label1) where n.idStr='b' return n")
//      res2.show})
//
//    val res3 = graphFacade.cypher("match (n:label0) where n.flag = false return n limit 10")
//    Profiler.timing({
//    val res4 = graphFacade.cypher("match (n:label0) where n.flag = false return n limit 10")
//    res4.show})
//    Profiler.timing({
//    val res5 = graphFacade.cypher("match (n:label0) where n.flag = false and n.id_p=70 return n limit 10")
//    val res6 = graphFacade.cypher("match (n:label0) where n.flag = false and n.id_p=70 return n limit 10")
//    res6.show})
//    Profiler.timing({
//    val res7 = graphFacade.cypher("match (n:label0) where n.flag = false and n.idStr='ha' return n limit 10")
//    val res8 = graphFacade.cypher("match (n:label0) where n.flag = false and n.idStr='ha' return n limit 10")
//    res8.show})
//    Profiler.timing({
//    val res9 = graphFacade.cypher("match (n:label0) where n.flag = false and n.idStr='ea' and n.id_p=40 return n limit 10")
//    val res10 = graphFacade.cypher("match (n:label0) where n.flag = false and n.idStr='ea' and n.id_p=40 return n limit 10")
//    res10.show})
  }

  @Test
  def api2(): Unit = {
    Profiler.timing({
      println("Test preheat")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false) {
          res += node
          count += 1
        }
      }
    })

    Profiler.timing({
      println("Test match (n:label0) where n.idStr='ha' return n")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("idStr")
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == "ha") {
          res += node
        }
      }
    })

//    Profiler.timing({
//      println("Test match (n:label0) where n.idStr='ha' return n")
//      val label = nodeStore.getLabelId("label0")
//      val prop = nodeStore.getPropertyKeyId("idStr")
//      val nodes = nodeStore.getNodesByLabel(label).map(mapNode)
//      val res = ArrayBuffer[Node[Long]]()
//      while (nodes.hasNext) {
//        val node = nodes.next()
//        val value = node.properties.value
////        if (node.properties.value) {
//////          res += node
////        }
//      }
//    })
//
//    Profiler.timing({
//      println("Test match (n:label0) where n.idStr='ha' return n")
//      val label = nodeStore.getLabelId("label0")
//      val prop = nodeStore.getPropertyKeyId("idStr")
//      nodeStore
//        .getNodesByLabel(label)
//        .map(mapNode)
//        .toIterable
//        .iterator
//        .filter{
//        node =>
//          node.properties.getOrElse("idStr", null).value == "ha"
//      }.foreach(println)
//    })

//    Profiler.timing({
//      println("Test match (n:label0) where n.idStr='ha' return n")
//      val label = nodeStore.getLabelId("label0")
//      val prop = nodeStore.getPropertyKeyId("idStr")
//      nodeStore
//        .getNodesByLabel(label)
//        .map(mapNode)
//        .filter{
//          node =>
//            node.properties.getOrElse("idStr", null).value == "ha"
//        }.foreach(println)
//    })
  }





  protected def mapRelation(rel: StoredRelation): Relationship[Long] = {
    new Relationship[Long] {
      override type I = this.type

      override def id: Long = rel.id

      override def startId: Long = rel.from

      override def endId: Long = rel.to

      override def relType: String = relationStore.getRelationTypeName(rel.typeId).get

      override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = Map.empty[String, Any]
        rel match {
          case rel: StoredRelationWithProperty =>
            props = rel.properties.map(kv => (relationStore.getRelationTypeName(kv._1).getOrElse("unknown"), kv._2))
          case _ =>
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  protected def mapNode2(node: StoredNode): Node[Long] = {
    new Node[Long] {
      override type I = this.type

      override def id: Long = node.id

      override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeStore.getLabelName(id).get)

      override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

      override def properties: CypherMap = {
//        var props: Map[String, Any] = Map.empty[String, Any]
//        node match {
//          case node: StoredNodeWithProperty =>
//            props = node.properties.map {
//              kv =>
//                (nodeStore.getPropertyKeyName(kv._1).get, kv._2)
//            }
//          case _ =>
//            val n = nodeStore.getNodeById(node.id)
//            props = n.asInstanceOf[StoredNodeWithProperty].properties.map {
//              kv =>
//                (nodeStore.getPropertyKeyName(kv._1).get, kv._2)
//            }
//        }
        CypherMap(node.asInstanceOf[StoredNodeWithProperty].properties.map(kv => (nodeStore.getPropertyKeyName(kv._1).get, kv._2)).toSeq: _*)
      }
    }
  }
  case class NodeId(value: Long) extends LynxId {

  }

  protected def mapNode(node: StoredNode): LynxNode = {
    new LynxNode {
      override val id: LynxId = NodeId(node.id)

      override def labels: Seq[String] = node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq

      override def property(name: String): Option[LynxValue] =
        node.asInstanceOf[StoredNodeWithProperty].properties.get(nodeStore.getPropertyKeyId(name)).map(LynxValue(_))
    }
  }

  @Test
  def mapNodeTest(): Unit ={
    val keyMap = Map(0->"idStr0", 1-> "idStr1", 2-> "idStr2", 3->"idStr3", 4 ->"idStr4")
    val t0 = System.currentTimeMillis()
    val storedNodes = nodeStore.allNodes().take(1000000).toArray
    val t1 = System.currentTimeMillis()
    val nodes = storedNodes.map(mapNode).map(_.property("idStr"))
    val t2 = System.currentTimeMillis()
    val nodes1 = storedNodes.map(node => CypherMap(node.properties.map(kv => (nodeStore.getPropertyKeyName(kv._1).get, kv._2)).toSeq: _*))
    val t3 = System.currentTimeMillis()
    val nodes2 = storedNodes.map(node => node.properties.map(kv => (nodeStore.getPropertyKeyName(kv._1).get, kv._2)))
    val t4 = System.currentTimeMillis()
    val nodes3 = storedNodes.map(node => node.properties.map(kv => (keyMap(kv._1), kv._2)))
    val t5 = System.currentTimeMillis()
    println("scan time: ", t1 - t0)
    println("mapNode time: ", t2 - t1)
    println("CypherMap time: ", t3 - t2)
    println("MapProps time: ", t4 - t3)
    println("MapProps without API time: ", t5 - t4)
  }

  @Test
  def relationAPI(): Unit ={
    println("match (n:label0)-[r]->(m:label1) return r limit 10000")

    val label0 = nodeStore.getLabelId("label0")
    val label1 = nodeStore.getLabelId("label1")
    val limit  = 10000

    val res = nodeStore
      .getNodeIdsByLabel(label0)
      .flatMap(relationStore.findOutRelations)
      .filter{
        rel =>
          nodeStore
            .getNodeById(rel.to)
            .exists(_.labelIds.contains(label1))
      }
      .take(limit)
    println(res.length)
  }

}