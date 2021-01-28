package cn.pandadb.kv.performance

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.junit.{Before, Test}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Relationship}

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
  var graphFacade: GraphFacade = _


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


  @Before
  def setup(): Unit = {

    val dbPath = "D:\\data\\graph500\\db"
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
  def relationAPITest(): Unit ={
    Profiler.timing({
      println("Test preheat")
      nodeStore.allNodes().take(1000)
      relationStore.allRelations().take(1000)
    })

    Profiler.timing({
      println("match (n:label0)-[r]->(m:label1) return r limit 10000")

      val label0 = nodeStore.getLabelId("label0")
      val label1 = nodeStore.getLabelId("label1")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
        .map(rel => mapRelation(relationStore.getRelationById(rel.id).get))
      println(res.length)
    })


    Profiler.timing({
      println("match (n:label0)-[r1]->(m:label1)-[r2]->(p:label2) return r2")

      val label0 = nodeStore.getLabelId("label2")
      val label1 = nodeStore.getLabelId("label3")
      val label2 = nodeStore.getLabelId("label4")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .map(_.to)
        .filter(nodeId =>nodeStore.hasLabel(nodeId, label1))
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label2))
        .take(limit)
        .map(rel => mapRelation(relationStore.getRelationById(rel.id).get))


      println(res.length)
    })

    Profiler.timing({
      println("match (n:label0)-[r1]->(m:label1)-[r2]->(p:label2)-[r3]->(q:label3) return r3 limit 10000")

      val label0 = nodeStore.getLabelId("label5")
      val label1 = nodeStore.getLabelId("label6")
      val label2 = nodeStore.getLabelId("label7")
      val label3 = nodeStore.getLabelId("label8")
      val limit = 10000

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
        .map(rel => mapRelation(relationStore.getRelationById(rel.id).get))

      println(res.length)
    })

    Profiler.timing({
      println("match (n:label0)-[r:type0]->(m:label1) return r")

      val label0 = nodeStore.getLabelId("label0")
      val label1 = nodeStore.getLabelId("label2")
      val type0  = relationStore.getRelationTypeId("type0")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(nodeId => relationStore.findOutRelations(nodeId, Some(type0)))
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
        .map(rel => mapRelation(relationStore.getRelationById(rel.id).get))

      println(res.length)
    })

    Profiler.timing({
      println("match (n:label2)-[r]->(m:label4) return r limit 10000")

      val label0 = nodeStore.getLabelId("label2")
      val label1 = nodeStore.getLabelId("label4")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
        .map(rel => mapRelation(relationStore.getRelationById(rel.id).get))
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
