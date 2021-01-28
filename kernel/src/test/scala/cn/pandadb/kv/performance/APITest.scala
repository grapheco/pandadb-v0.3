package cn.pandadb.kv.performance

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeId, NodeStoreSPI, PandaNode, RelationStoreSPI, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.grapheco.lynx.{LynxId, LynxNode, LynxValue, NodeFilter, RelationshipFilter}
import org.junit.{Before, Test}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}
import org.opencypher.v9_0.expressions.SemanticDirection

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
  var graphFacade: GraphFacade = _


  @Before
  def setup(): Unit = {

    val dbPath = "C:\\PandaDB_rocksDB\\graph500"
    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  @Test
  def createIndex(): Unit ={
    Profiler.timing({
      graphFacade.createIndexOnNode("label1", Set("idStr"))
    })
  }

  @Test
  def relsTest(): Unit = {
    val limit = 100000
    Profiler.timing({
      println("rels without any nodes")
      val res = graphFacade.rels(includeStartNodes = false, includeEndNodes = false).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("rels with start node")
      val res = graphFacade.rels(includeStartNodes = true, includeEndNodes = false).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("rels with both nodes")
      val res = graphFacade.rels(includeStartNodes = true, includeEndNodes = true).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("nodes")
      val res = graphFacade.nodes().take(limit)
      println(res.length)
    })
  }

  @Test
  def nodeTest(): Unit ={
    Profiler.timing({
      println("(label2{idStr:'fcic'})")
      val res = graphFacade.nodes(NodeFilter(Seq("label2"), Map("idStr"->LynxValue("fcic")))).take(10)
      println(res.length)
    })
    Profiler.timing({
      println("(label1{idStr:'bgggbab'})")
      val res = graphFacade.nodes(NodeFilter(Seq("label1"), Map("idStr"->LynxValue("bgggbab")))).take(10)
      println(res.length)
    })
  }

  @Test
  def pathTest(): Unit ={
    val limit = 1000
    Profiler.timing({
      println("(3543605)-[]->(*)")
      val res = graphFacade.paths(NodeId(3543605),SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(3543605)<-[]-(*)")
      val res = graphFacade.paths(NodeId(3543605),SemanticDirection.INCOMING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(1298177)-[:type7]->(*)")
      val res = graphFacade
        .paths(NodeId(1298177),
          RelationshipFilter(Seq("type7"),Map()),
          NodeFilter(Seq(), Map()),
          SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(2925502)-[:type2]->(:label1)")
      val res = graphFacade
        .paths(NodeId(2925502),
          RelationshipFilter(Seq("type2"),Map()),
          NodeFilter(Seq("label1"), Map()),
          SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(5282)-[:type2]->(:label1{flag:false})")
      val res = graphFacade
        .paths(NodeId(5282),
          RelationshipFilter(Seq("type2"),Map()),
          NodeFilter(Seq("label1"), Map("flag"->LynxValue(false))),
          SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(3583116)-[]->(:label1{flag:false})")
      val res = graphFacade
        .paths(NodeId(3583116),
          RelationshipFilter(Seq(),Map()),
          NodeFilter(Seq("label1"), Map("flag"->LynxValue(false))),
          SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(label0)-[]->()")
      val res = graphFacade
        .paths(
          NodeFilter(Seq("label0"), Map()),
          RelationshipFilter(Seq(),Map()),
          NodeFilter(Seq(), Map()),
          SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(label0)-[type0]->()")
      val res = graphFacade
        .paths(
          NodeFilter(Seq("label0"), Map()),
          RelationshipFilter(Seq("type0"),Map()),
          NodeFilter(Seq(), Map()),
          SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(label2{idStr:'fcic'})-[]->()")
      val res = graphFacade
        .paths(
          NodeFilter(Seq("label2"), Map("idStr"->LynxValue("fcic"))),
          RelationshipFilter(Seq(),Map()),
          NodeFilter(Seq(), Map()),
          SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(label1{idStr:'bgggbab'})-[]->() with index")
      val res = graphFacade
        .paths(
          NodeFilter(Seq("label1"), Map("idStr"->LynxValue("bgggbab"))),
          RelationshipFilter(Seq(),Map()),
          NodeFilter(Seq(), Map()),
          SemanticDirection.OUTGOING).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(label0)-[type0]->(label1)-[type1]->(label2)-[type2]->(label3)")
      val res = graphFacade
        .paths(
          NodeFilter(Seq("label0"), Map()),
          (RelationshipFilter(Seq("type0"),Map()),NodeFilter(Seq("label1"), Map()), SemanticDirection.OUTGOING),
          (RelationshipFilter(Seq("type1"),Map()),NodeFilter(Seq("label2"), Map()), SemanticDirection.OUTGOING),
          (RelationshipFilter(Seq("type2"),Map()),NodeFilter(Seq("label3"), Map()), SemanticDirection.OUTGOING),
          ).take(limit)
      println(res.length)
    })
    Profiler.timing({
      println("(label0)-[type0]->(label1)<-[type1]-(label2)-[type2]-(label3)")
      val res = graphFacade
        .paths(
          NodeFilter(Seq("label0"), Map()),
          (RelationshipFilter(Seq("type0"),Map()),NodeFilter(Seq("label1"), Map()), SemanticDirection.OUTGOING),
          (RelationshipFilter(Seq("type1"),Map()),NodeFilter(Seq("label2"), Map()), SemanticDirection.INCOMING),
          (RelationshipFilter(Seq("type2"),Map()),NodeFilter(Seq("label3"), Map()), SemanticDirection.BOTH),
        ).take(limit)
      println(res.length)
    })
  }

  def mapNode(node: StoredNode): PandaNode = {
    PandaNode(node.id,
      node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq,
      node.properties.map(kv=>(nodeStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq: _*)
  }

  @Test
  def mapNodeTest(): Unit = {
    val t0 = System.currentTimeMillis()
    val storedNodes = nodeStore.allNodes().take(1000000).toArray
    val t1 = System.currentTimeMillis()
    val nodes = storedNodes.map(mapNode).map(_.properties)
    val t2 = System.currentTimeMillis()
    println("scan time: ", t1 - t0)
    println("mapNode time: ", t2 - t1)
    println(nodes.head)
  }

  @Test
  def mytest(): Unit ={
    test(1, println)
    test("1", println)
    test(1.1, println)
  }

  def test[T](value: T, function: T=>Unit): Unit ={
    function(value)
  }

}