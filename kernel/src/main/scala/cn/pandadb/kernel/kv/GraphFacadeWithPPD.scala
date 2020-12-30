package cn.pandadb.kernel.kv

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.{NameStore, Statistics}
import cn.pandadb.kernel.optimizer.PandaCypherSession
import cn.pandadb.kernel.store._
import org.apache.logging.log4j.scala.Logging
import org.opencypher.lynx.LynxSession
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

class GraphFacadeWithPPD(
                          nodeIdGen: FileBasedIdGen,
                          relIdGen: FileBasedIdGen,
                          nodeStore: NodeStoreSPI,
                          relationStore: RelationStoreSPI,
                          indexStore: IndexStoreAPI,
                          statistics: Statistics,
                          onClose: => Unit
                 ) extends Logging with GraphService {

  private val propertyGraph = new PandaCypherSession().createPropertyGraph(
    new PandaPropertyGraphScanImpl(nodeStore, relationStore, indexStore, statistics),
    new PandaPropertyGraphWriterImpl(nodeIdGen, relIdGen, nodeStore, relationStore, indexStore, statistics)
  )


  init()

  override def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = {
    propertyGraph.cypher(query, CypherMap(parameters.toSeq: _*))
  }

  override def close(): Unit = {
    nodeIdGen.flush()
    relIdGen.flush()
    statistics.flush()
    nodeStore.close()
    relationStore.close()
    indexStore.close()
    statistics.close()
    onClose
  }

  override def addNode(nodeProps: Map[String, Any], labels: String*): this.type = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = nodeStore.getLabelIds(labels.toSet).toArray
    val props = nodeProps.map{
      v => ( nodeStore.getPropertyKeyId(v._1),v._2)
    }
    nodeStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, props))
    statistics.increaseNodeCount(1) // TODO batch
    labelIds.foreach(statistics.increaseNodeLabelCount(_, 1))
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
    statistics.increaseRelationCount(1) // TODO batch , index statistic
    statistics.increaseRelationTypeCount(labelId, 1)
    this
  }

  override def deleteNode(id: Id): this.type = {
    nodeStore.getNodeById(id).foreach{
      node =>
        nodeStore.deleteNode(node.id)
        statistics.decreaseNodes(1)
        node.labelIds.foreach(statistics.decreaseNodeLabelCount(_, 1))
    }
    this
  }

  override def deleteRelation(id: Id): this.type = {
    relationStore.getRelationById(id).foreach{
      rel =>
        relationStore.deleteRelation(rel.id)
        statistics.decreaseRelations(1)
        statistics.setRelationTypeCount(rel.typeId, 1)
    }
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

  override def nodeSetProperty(id: Id, key: String, value: Any): Unit =
    nodeStore.nodeSetProperty(id, nodeStore.getPropertyKeyId(key), value)

  override def nodeRemoveProperty(id: Id, key: String): Unit =
    nodeStore.nodeRemoveProperty(id, nodeStore.getPropertyKeyId(key))

  override def nodeAddLabel(id: Id, label: String): Unit =
    nodeStore.nodeAddLabel(id, nodeStore.getLabelId(label))

  override def nodeRemoveLabel(id: Id, label: String): Unit =
    nodeStore.nodeRemoveLabel(id, nodeStore.getLabelId(label))

  override def relationSetProperty(id: Id, key: String, value: Any): Unit =
    relationStore.relationSetProperty(id, relationStore.getPropertyKeyId(key), value)

  override def relationRemoveProperty(id: Id, key: String): Unit =
    relationStore.relationRemoveProperty(id, relationStore.getPropertyKeyId(key))

  override def relationAddLabel(id: Id, label: String): Unit = ???

  override def relationRemoveLabel(id: Id, label: String): Unit = ???

  //FIXME: expensive time cost
  private def init(): Unit = {

  }

  def snapshot(): Unit = {
    //TODO: transaction safe
  }
}
