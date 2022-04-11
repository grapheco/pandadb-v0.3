package org.grapheco.pandadb.kernel.distribute

import java.nio.ByteBuffer
import org.grapheco.pandadb.kernel.distribute.index.{IndexStoreService, PandaDistributedIndexStore}
import org.grapheco.pandadb.kernel.distribute.meta.{DistributedStatistics, NameMapping, PropertyNameStore}
import org.grapheco.pandadb.kernel.distribute.node.NodeStoreAPI
import org.grapheco.pandadb.kernel.distribute.relationship.RelationStoreAPI
import org.grapheco.pandadb.kernel.store.{NodeId, PandaNode, PandaRelationship, RelationId, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.grapheco.pandadb.kernel.util.PandaDBException.PandaDBException
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.grapheco.lynx.{CypherRunner, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType, LynxResult, LynxValue, NodeFilter}
import org.tikv.common.{TiConfiguration, TiSession}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 16:45
 */
class DistributedGraphFacade(kvHosts: String, indexHosts: String, udpClientManager: UDPClientManager) extends DistributedGraphService {

  val db = {
    val conf = TiConfiguration.createRawDefault(kvHosts)
    val session = TiSession.create(conf)
    new PandaDistributeKVAPI(session.createRawClient())
  }
  val propertyNameStore = new PropertyNameStore(db, udpClientManager)
  val nodeStoreAPI = new NodeStoreAPI(db, propertyNameStore)
  val relationStoreAPI = new RelationStoreAPI(db, propertyNameStore)

  val statistics = new DistributedStatistics(db)
  statistics.init()

  val indexStore: IndexStoreService = {
    val hosts = indexHosts.split(",").map(ipAndPort => {
      val ip = ipAndPort.split(":")
      new HttpHost(ip(0), ip(1).toInt, "http")
    })
    new PandaDistributedIndexStore(new RestHighLevelClient(RestClient.builder(hosts: _*)), db, this, udpClientManager)
  }
  val cypherParseModel = new GraphParseModel(this)
  val runner = new CypherRunner(cypherParseModel)
  udpClientManager.setDB(this)


  override def cleanDB(): Unit = {
    statistics.clean()
    propertyNameStore.cleanData()
    nodeStoreAPI.cleanData()
    relationStoreAPI.cleanData()
    val left = ByteBuffer.wrap(Array((0).toByte)).array()
    val right = ByteBuffer.wrap(Array((-1).toByte)).array()
    db.deleteRange(left, right)
    indexStore.cleanIndexes(NameMapping.indexName)
  }

  override def getStatistics: DistributedStatistics = statistics

  override def getIndexStore: IndexStoreService = indexStore

  override def cypher(query: String, parameters: Map[String, Any]): LynxResult = {
    runner.compile(query)
    runner.run(query, parameters)
  }
  override def refreshMeta(): Unit = {
    synchronized {
      nodeStoreAPI.refreshMeta()
      relationStoreAPI.refreshMeta()
      indexStore.refreshIndexMeta()
      statistics.refreshMeta()
    }
  }
  override def close(): Unit = {
    nodeStoreAPI.close()
    relationStoreAPI.close()
    statistics.flush()
  }

  override def newNodeId(): Id = nodeStoreAPI.newNodeId()

  override def newRelationshipId(): Id = relationStoreAPI.newRelationId()

  override def getNodeLabelId(labelName: String): Option[Int] = nodeStoreAPI.getLabelId(labelName)

  override def getNodeLabelName(labelId: Int): Option[String] = nodeStoreAPI.getLabelName(labelId)

  override def getRelationTypeId(typeName: String): Option[Int] = relationStoreAPI.getRelationTypeId(typeName)

  override def getRelationTypeName(typeId: Int): Option[String] = relationStoreAPI.getRelationTypeName(typeId)

  override def getPropertyName(propertyId: Int): Option[String] = propertyNameStore.key(propertyId)

  override def getPropertyId(propertyName: String): Option[Int] = propertyNameStore.id(propertyName)

  override def createIndexOnNode(label: String, props: Set[String]): Unit = {
    val created = indexStore.isIndexCreated(label, props.toSeq)
    if (!created) {
      val iter = getNodesByLabel(label, false)
      indexStore.batchAddIndexOnNodes(label, props.toSeq, iter)
    }
    udpClientManager.sendRefreshMsg()
  }

  override def dropIndexOnNode(label: String, prop: String): Unit = {
    val created = indexStore.isIndexCreated(label, Seq(prop))
    if (created) {
      val nodes = getNodesByLabel(label, false)
      indexStore.batchDropIndexLabelWithProperty(label, prop, nodes)
    }
    udpClientManager.sendRefreshMsg()
  }

  override def getNodesByIndex(nodeFilter: NodeFilter): Iterator[PandaNode] =
    indexStore.searchNodes(
      nodeFilter.labels.map(_.value), nodeFilter.properties.map(p => p._1.value -> p._2.value)
    ).flatten.map(f => getNodeById(f, nodeFilter.labels.head.value).get)

  override def addNode(nodeProps: Map[String, Any], labels: String*): Id = {
    addNode(None, labels, nodeProps)
  }

  override def addNode(nodeId: Id, nodeProps: Map[String, Any], labels: String*): Id = {
    addNode(Option(nodeId), labels, nodeProps)
  }

  override def addNodes(nodes: Iterator[PandaNode]): Unit = {
    //TODO: opt
    nodes.foreach(node => addNode(node.id.value, node.props.map(f => (f._1.value, f._2.value)), node.labels.map(_.value):_*))
  }

  private def addNode(id: Option[Long], labels: Seq[String], nodeProps: Map[String, Any]): Id = {
    val nodeId = id.getOrElse(nodeStoreAPI.newNodeId())
    val labelIds = labels.map(label => nodeStoreAPI.addLabel(label)).toArray
    val properties = nodeProps.map(kv => (nodeStoreAPI.addPropertyKey(kv._1), kv._2))
    nodeStoreAPI.addNode(new StoredNodeWithProperty(nodeId, labelIds, properties))
    // if property has index
    indexStore.setIndexOnSingleNode(nodeId, labels, nodeProps)
    statistics.increaseNodeCount(1)
    labelIds.foreach(lid => statistics.increaseNodeLabelCount(lid, 1))
    nodeId
  }

  override def deleteNode(id: Id): Unit = {
    val node = getNodeById(id).get

    nodeStoreAPI.deleteNode(id)
    if (indexStore.getIndexTool.isDocExist(id.toString)) indexStore.deleteNodeByNodeId(id.toString)
    statistics.decreaseNodes(1)
    node.labels.map(l => nodeStoreAPI.getLabelId(l.value).get).foreach(lid => statistics.decreaseNodeLabelCount(lid, 1))
  }

  override def deleteNodes(ids: Iterator[Id]): Unit = {
    //TODO: opt
    ids.foreach(id => deleteNode(id))
  }

  override def nodeSetProperty(id: Id, key: String, value: Any): Unit = {
    val pid = nodeStoreAPI.addPropertyKey(key)
    nodeStoreAPI.nodeSetProperty(id, pid, value)
    val node = getNodeById(id).get
    indexStore.setIndexOnSingleNode(node.id.value, node.labels.map(_.value), node.props.map(p => p._1.value -> p._2.value))
  }

  override def nodeRemoveProperty(id: Id, key: String): Unit = {
    val pid = nodeStoreAPI.getPropertyKeyId(key)
    if (pid.isDefined) {
      nodeStoreAPI.nodeRemoveProperty(id, pid.get)
      val node = getNodeById(id).get
      if (node.props.nonEmpty) indexStore.dropIndexOnSingleNodeProp(node.id.value, node.labels.map(_.value), key)
      else indexStore.deleteNodeByNodeId(node.id.value.toString)
    }
  }

  override def nodeAddLabel(id: Id, label: String): Unit = {
    val lid = nodeStoreAPI.addLabel(label)
    nodeStoreAPI.nodeAddLabel(id, lid)
    val node = getNodeById(id).get
    indexStore.setIndexOnSingleNode(node.id.value, node.labels.map(_.value), node.props.map(p => p._1.value -> p._2.value))
    statistics.increaseNodeLabelCount(lid, 1)
  }

  override def nodeRemoveLabel(id: Id, label: String): Unit = {
    val lid = nodeStoreAPI.getLabelId(label)
    lid.foreach(lid => {
      nodeStoreAPI.nodeRemoveLabel(id, lid)
      val node = getNodeById(id).get
      node.labels.size match {
        case 0 => indexStore.deleteNodeByNodeId(node.id.value.toString)
        case _ => indexStore.dropIndexOnSingleNodeLabel(node.id.value, label)
      }
      statistics.decreaseNodeLabelCount(lid, 1)
    })
  }

  override def getNodeById(id: Id): Option[PandaNode] = nodeStoreAPI.getNodeById(id).map(mapNode(_))

  override def getNodeById(id: Id, labelName: String): Option[PandaNode] =  nodeStoreAPI.getNodeById(id, getNodeLabelId(labelName)).map(mapNode(_))

  override def getNodesByIds(ids: Seq[Id], labelName: String): Seq[PandaNode] = {
    nodeStoreAPI.getNodesByIds(getNodeLabelId(labelName).get, ids).map(mapNode).toSeq
  }

  override def getNodesByIds(ids: Seq[Id], labelId: Int): Seq[PandaNode] = {
    nodeStoreAPI.getNodesByIds(labelId, ids).map(mapNode).toSeq

  }

  override def getNodesByLabel(labelName: String, exact: Boolean): Iterator[PandaNode] = {
    val lid = getNodeLabelId(labelName)
    if (lid.isDefined) nodeStoreAPI.getNodesByLabel(lid.get).map(mapNode)
    else Iterator.empty
  }

  override def scanAllNodes(): Iterator[PandaNode] = nodeStoreAPI.allNodes().map(mapNode)

  private def mapNode(node: StoredNode): PandaNode = {
    PandaNode(NodeId(node.id),
      node.labelIds.map((id: Int) => nodeStoreAPI.getLabelName(id).get).toSeq.map(LynxNodeLabel),
      node.properties.map(kv => (LynxPropertyKey(nodeStoreAPI.getPropertyKeyName(kv._1).getOrElse("unknown")), LynxValue(kv._2))))
  }

  override def addRelation(typeName: String, from: Id, to: Id, relProps: Map[String, Any]): Id =
    addRelation(None, typeName, from, to, relProps)

  override def addRelation(relId: Id, typeName: String, from: Id, to: Id, relProps: Map[String, Any]): Id =
    addRelation(Option(relId), typeName, from, to, relProps)

  override def addRelations(relationships: Iterator[PandaRelationship]): Unit =
    relationships.foreach(r => addRelation(r.id.value, r.relationType.get.value, r.startNodeId.value, r.endNodeId.value, r.props.map(f => (f._1.value, f._2.value))))

  private def addRelation(id: Option[Long], label: String, from: Long, to: Long, relProps: Map[String, Any]): Id = {
    val rid = id.getOrElse(relationStoreAPI.newRelationId())
    val labelId = relationStoreAPI.addRelationType(label)
    val props = relProps.map(v => (relationStoreAPI.addPropertyKey(v._1), v._2))
    val rel = new StoredRelationWithProperty(rid, from, to, labelId, props)
    relationStoreAPI.addRelation(rel)
    statistics.increaseRelationCount(1)
    statistics.increaseRelationTypeCount(labelId, 1)
    rid
  }

  override def deleteRelation(id: Id): Unit = {
    val relation = relationStoreAPI.getRelationById(id)
    if (relation.isDefined) {
      relationStoreAPI.deleteRelation(id)
      statistics.decreaseRelations(1)
      statistics.decreaseRelationLabelCount(relation.get.typeId, 1)
    }
  }

  override def deleteRelations(ids: Iterator[Id]): Unit = {
    // TODO: opt
    ids.foreach(id => deleteRelation(id))
  }

  override def relationSetProperty(id: Id, key: String, value: Any): Unit =
    relationStoreAPI.relationSetProperty(id, relationStoreAPI.addPropertyKey(key), value)

  override def relationRemoveProperty(id: Id, key: String): Unit =
    relationStoreAPI.getPropertyKeyId(key).foreach(kid => relationStoreAPI.relationRemoveProperty(id, kid))

  override def relationAddType(id: Id, label: String): Unit = throw new PandaDBException("not support add relationship type")

  override def relationRemoveType(id: Id, label: String): Unit = throw new PandaDBException("not support remove relationship type")

  override def getRelationById(id: Id): Option[PandaRelationship] = relationStoreAPI.getRelationById(id).map(mapRelation)

  override def scanAllRelations(): Iterator[PandaRelationship] = relationStoreAPI.allRelations().map(mapRelation)

  override def findOutRelations(fromNodeId: Id, edgeType: Option[Int]): Iterator[PandaRelationship] =
    relationStoreAPI.findOutRelations(fromNodeId, edgeType).map(mapRelation)

  override def findInRelations(toNodeId: Id, edgeType: Option[Int]): Iterator[PandaRelationship] =
    relationStoreAPI.findInRelations(toNodeId, edgeType).map(mapRelation)

  // ====================================== new added ======================================
  override def countOutRelations(fromNodeId: Id): Long = relationStoreAPI.countOutRelations(fromNodeId)

  override def countOutRelations(fromNodeId: Id, edgeType: Int): Long = relationStoreAPI.countOutRelations(fromNodeId, edgeType)

  override def findOutRelationsEndNodeIds(fromNodeId: Id): Iterator[Id] = relationStoreAPI.findOutRelationsEndNodeIds(fromNodeId)

  override def findOutRelationsEndNodeIds(fromNodeId: Id, edgeType: Int): Iterator[Id] = relationStoreAPI.findOutRelationsEndNodeIds(fromNodeId, edgeType)

  // =======================================================================================

  private def mapRelation(rel: StoredRelation): PandaRelationship = {
    PandaRelationship(RelationId(rel.id),
      NodeId(rel.from), NodeId(rel.to),
      relationStoreAPI.getRelationTypeName(rel.typeId).map(LynxRelationshipType),
      rel.properties.map(kv => (LynxPropertyKey(relationStoreAPI.getPropertyKeyName(kv._1).getOrElse("unknown")), LynxValue(kv._2))))
  }

}
