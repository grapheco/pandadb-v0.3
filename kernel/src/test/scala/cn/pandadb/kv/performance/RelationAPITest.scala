package cn.pandadb.kv.performance

import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.junit.{Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @ClassName RelationAPITest
 * @Description TODO
 * @Author huchuan
 * @Date 2021/1/4
 * @Version 0.1
 */
class RelationAPITest {

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacadeWithPPD = _


  @Before
  def setup(): Unit = {

    val dbPath = "F:\\PandaDB_rocksDB\\10kw"
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
  def relationAPITest(): Unit ={
    Profiler.timing({
      println("Test preheat")
      nodeStore.allNodes().take(10000)
      relationStore.allRelations().take(10000)
    })

    Profiler.timing({
      println("match (n:label0)-[r]->(m:label1) return r limit 10000")

      val label0 = nodeStore.getLabelId("label5")
      val label1 = nodeStore.getLabelId("label6")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
      println(res.length)
    })


    Profiler.timing({
      println("match (n:label0)-[r1]->(m:label1)-[r2]->(p:label2) return r2")

      val label0 = nodeStore.getLabelId("label0")
      val label1 = nodeStore.getLabelId("label1")
      val label2 = nodeStore.getLabelId("label2")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .map(_.to)
        .filter(nodeId =>nodeStore.hasLabel(nodeId, label1))
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label2))
        .take(limit)

      println(res.length)
    })

    Profiler.timing({
      println("match (n:label0)-[r1]->(m:label1)-[r2]->(p:label2)-[r3]->(q:label3) return r3 limit 1000")

      val label0 = nodeStore.getLabelId("label0")
      val label1 = nodeStore.getLabelId("label1")
      val label2 = nodeStore.getLabelId("label2")
      val label3 = nodeStore.getLabelId("label3")
      val limit = 1000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .map(_.to)
        .filter(nodeId =>nodeStore.hasLabel(nodeId, label1))
        .flatMap(relationStore.findOutRelations)
        .map(_.to)
        .filter(nodeId =>nodeStore.hasLabel(nodeId, label2))
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label3))
        .take(limit)
      println(res.length)
    })

    Profiler.timing({
      println("match (n:label0)-[r:type0]->(m:label1) return r")

      val label0 = nodeStore.getLabelId("label0")
      val label1 = nodeStore.getLabelId("label1")
      val type0  = relationStore.getRelationTypeId("type0")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(nodeId => relationStore.findOutRelations(nodeId, type0))
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
      println(res.length)
    })
//
//    Profiler.timing({
//      println("match (n:label0)-[r:type1]->(m:label1) return r")
//
//      val label0 = nodeStore.getLabelId("label0")
//      val label1 = nodeStore.getLabelId("label1")
//      val type1  = relationStore.getRelationTypeId("type1")
//      val limit = 10000
//
//      val res = nodeStore
//        .getNodeIdsByLabel(label0)
//        .flatMap(relationStore.findOutRelations)
//        .filter(_.typeId == type1)
//        .filter{
//          rel =>
//            nodeStore
//              .getNodeById(rel.to)
//              .exists(_.labelIds.contains(label1))
//        }
//        .take(limit)
//      println(res.length)
//    })
//
//    Profiler.timing({
//      println("match (n:label3)-[r]->(m:label6) return r")
//
//      val label3 = nodeStore.getLabelId("label3")
//      val label6 = nodeStore.getLabelId("label6")
//      val limit = 10000
//
//      val res = nodeStore
//        .getNodeIdsByLabel(label3)
//        .flatMap(relationStore.findOutRelations)
//        .filter{
//          rel =>
//            nodeStore
//              .getNodeById(rel.to)
//              .exists(_.labelIds.contains(label6))
//        }
//        .take(limit)
//      println(res.length)
//    })
//
//    Profiler.timing({
//      println("match (n:label3)-[r:type1]->(m:label6) return r")
//
//      val label3 = nodeStore.getLabelId("label3")
//      val label6 = nodeStore.getLabelId("label6")
//      val type1  = relationStore.getRelationTypeId("type1")
//      val limit = 10000
//
//      val res = nodeStore
//        .getNodeIdsByLabel(label3)
//        .flatMap(relationStore.findOutRelations)
//        .filter(_.typeId == type1)
//        .filter{
//          rel =>
//            nodeStore
//              .getNodeById(rel.to)
//              .exists(_.labelIds.contains(label6))
//        }
//        .take(limit)
//      println(res.length)
//    })
  }
}
