package cn.pandadb.kernel.kv

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.NameStore
import cn.pandadb.kernel.optimizer.PandaCypherSession
import cn.pandadb.kernel.store._
import org.apache.logging.log4j.scala.Logging
import org.opencypher.lynx.{LynxSession, PropertyGraphScan}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

class GraphFacadeWithPPD(
                          nodeIdGen: FileBasedIdGen,
                          relIdGen: FileBasedIdGen,
                          nodeStore: NodeStoreSPI,
                          relationStore: RelationStoreSPI,
                          indexStore: IndexStoreAPI,
                          onClose: => Unit
                 ) extends Logging with GraphService {

  private val propertyGraph = new PandaCypherSession().createPropertyGraph(
    new PandaPropertyGraphScanImpl(nodeIdGen, relIdGen, nodeStore, relationStore, indexStore)
  )


  init()

  override def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = {
    propertyGraph.cypher(query, CypherMap(parameters.toSeq: _*))
  }

  override def close(): Unit = {
    nodeIdGen.flush()
    relIdGen.flush()
    nodeStore.close()
    relationStore.close()
    indexStore.close()
    onClose
  }

  override def addNode(nodeProps: Map[String, Any], labels: String*): this.type = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = nodeStore.getLabelIds(labels.toSet).toArray
    val props = nodeProps.map{
      v => ( nodeStore.getPropertyKeyId(v._1),v._2)
    }
    nodeStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, props))

    this
  }

  def addNode2(nodeProps: Map[String, Any], labels: String*): Long = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = nodeStore.getLabelIds(labels.toSet).toArray
    val props = nodeProps.map{
      v => ( nodeStore.getPropertyKeyId(v._1),v._2)
    }
    nodeStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, props))
    nodeId
  }


  override def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]): this.type = {
    val rid = relIdGen.nextId()
    val labelId = relationStore.getRelationTypeId(label)
    val props = relProps.map{
      v => ( relationStore.getPropertyKeyId(v._1),v._2)
    }
    val rel = new StoredRelationWithProperty(rid, from, to, labelId, props)
    //TODO: transaction safe
    relationStore.addRelation(rel)
    this
  }

  override def deleteNode(id: Id): this.type = {
    nodeStore.deleteNode(id)
    this
  }

  override def deleteRelation(id: Id): this.type = {
    relationStore.deleteRelation(id)
    this
  }

  def allNodes(): Iterable[StoredNodeWithProperty] = {
    nodeStore.allNodes().toIterable
  }

  def allRelations(): Iterable[StoredRelation] = {
    relationStore.allRelations().toIterable
  }

  def createNodePropertyIndex(nodeLabel: String, propertyNames: Set[String]): Int = {
    val labelId = nodeStore.getLabelId(nodeLabel)
    val propNameIds = propertyNames.map(nodeStore.getPropertyKeyId)
    indexStore.createIndex(labelId, propNameIds.toArray)
  }

  def writeNodeIndexRecord(indexId: Int, nodeId: Long, propertyValue: Any): Unit = {
    indexStore.insertIndexRecord(indexId, propertyValue, nodeId)
  }

  //FIXME: expensive time cost
  private def init(): Unit = {

  }

  def snapshot(): Unit = {
    //TODO: transaction safe
  }
}
