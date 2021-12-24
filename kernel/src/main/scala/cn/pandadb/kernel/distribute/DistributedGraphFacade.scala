package cn.pandadb.kernel.distribute

import java.nio.ByteBuffer

import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import cn.pandadb.kernel.distribute.meta.{DistributedStatistics, NameMapping, PropertyNameStore}
import cn.pandadb.kernel.distribute.node.NodeStoreAPI
import cn.pandadb.kernel.distribute.relationship.RelationStoreAPI
import cn.pandadb.kernel.store.{PandaNode, PandaRelationship, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.grapheco.lynx.cypherplus.CypherRunnerPlus
import org.grapheco.lynx.{LynxResult, LynxTransaction, LynxValue, NodeFilter}
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 16:45
 */
class DistributedGraphFacade(kvHosts: String, indexHosts: String) extends DistributedGraphService {

  val db = {
    val conf = TiConfiguration.createRawDefault(kvHosts)

    val session = TiSession.create(conf)
    new PandaDistributeKVAPI(session.createRawClient())
  }

  val propertyNameStore = new PropertyNameStore(db)
  val nodeStore = new NodeStoreAPI(db, propertyNameStore)
  val relationStore = new RelationStoreAPI(db, propertyNameStore)

  val statistics = new DistributedStatistics(db)
  statistics.init()

  val indexStore = {
    val hosts = indexHosts.split(",").map(ipAndPort => {
      val ip = ipAndPort.split(":")
      new HttpHost(ip(0), ip(1).toInt, "http")
    })

    new PandaDistributedIndexStore(new RestHighLevelClient(RestClient.builder(hosts: _*)), db, nodeStore, statistics)
  }

  val runner = new CypherRunnerPlus(new GraphParseModel(this))

  def cleanDB(): Unit ={
    statistics.clean()
    propertyNameStore.cleanData()
    nodeStore.cleanData()
    relationStore.cleanData()
    val left = ByteBuffer.wrap(Array((0).toByte)).array()
    val right = ByteBuffer.wrap(Array((-1).toByte)).array()
    db.deleteRange(left, right)

    indexStore.cleanIndexes(NameMapping.indexName)
  }

  override def newNodeId(): Id = nodeStore.newNodeId()

  override def newRelationshipId(): Id = relationStore.newRelationId()

  override def addNode(nodeProps: Map[String, Any], labels: String*): Id = {
    addNode(None, labels, nodeProps)
  }

  override def addNode(nodeId: Id, nodeProps: Map[String, Any], labels: String*): Id = {
    addNode(Option(nodeId), labels, nodeProps)
  }

  private def addNode(id: Option[Long], labels: Seq[String], nodeProps: Map[String, Any]): Id = {
    val nodeId = id.getOrElse(nodeStore.newNodeId())
    val labelIds = labels.map(label => nodeStore.addLabel(label)).toArray
    val properties = nodeProps.map(kv => (nodeStore.addPropertyKey(kv._1), kv._2))
    nodeStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, properties))

    // if property has index
    indexStore.updateIndexOnSingleNode(nodeId, labels, nodeProps, (false, ""))

    statistics.increaseNodeCount(1)
    labelIds.foreach(lid => statistics.increaseNodeLabelCount(lid, 1))

    nodeId
  }

  override def getNodeById(id: Id): Option[PandaNode] = {
    nodeStore.getNodeById(id).map(mapNode(_))
  }

  override def getNodeById(id: Id, labelName: String): Option[PandaNode] = {
    nodeStore.getNodeById(id, nodeStore.getLabelId(labelName)).map(mapNode(_))
  }

  override def getNodesByLabel(labelNames: Seq[String], exact: Boolean): Iterator[PandaNode] = {
    val labelNotExist = labelNames.filter(labelName => getNodeLabelId(labelName).isEmpty)

    if (labelNotExist.isEmpty){
      labelNames.size match {
        case 0 => scanAllNode()
        case 1 => {
          val lid = nodeStore.getLabelId(labelNames.head).get
          nodeStore.getNodesByLabel(lid).map(mapNode(_))
        }
        case _ =>{
          val labelIds = labelNames.map(nodeStore.getLabelId(_).get).sorted
          val minLabel = labelIds.map(lid => lid -> statistics.getNodeLabelCount(lid).get).minBy(f => f._2)

          val res = nodeStore.getNodesByLabel(minLabel._1).filter {
            if (exact)
              _.labelIds.sorted.toSeq == labelIds
            else
              _.labelIds.sorted.containsSlice(labelIds)
          }
          res.map(mapNode(_))
        }
      }
    }
    else {
      throw new PandaDBException(s"label: ${labelNotExist.mkString(",")} not exist...")
    }
  }

  override def getNodeLabelId(labelName: String): Option[Int] = {
    nodeStore.getLabelId(labelName)
  }

  override def transferInnerNode(n: StoredNode): PandaNode = mapNode(n)

  override def scanAllNode(): Iterator[PandaNode] = {
    nodeStore.allNodes().map(mapNode(_))
  }

  override def deleteNode(id: Id): Unit = {
    val node = getNodeById(id).get

    nodeStore.deleteNode(id)
    if (indexStore.isDocExist(id.toString)) indexStore.deleteDoc(id.toString)

    statistics.decreaseNodes(1)
    node.labels.map(l => nodeStore.getLabelId(l).get).foreach(lid => statistics.decreaseNodeLabelCount(lid, 1))
  }


  override def deleteNodes(ids: Iterator[Id]): Unit = {
    // todo: batch delete
    ids.foreach(id => deleteNode(id))

  }

  protected def mapNode(node: StoredNode): PandaNode = {
    PandaNode(node.id,
      node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq,
      node.properties.map(kv => (nodeStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq: _*)
  }

  override def nodeSetProperty(id: Id, key: String, value: Any): Unit = {
    val pid = nodeStore.addPropertyKey(key)
    nodeStore.nodeSetProperty(id, pid, value)
    val node = getNodeById(id).get
    indexStore.updateIndexOnSingleNode(node.longId, node.labels, node.properties.map(p => p._1->p._2.value), (false, ""))
  }

  override def nodeRemoveProperty(id: Id, key: String): Unit = {
    val pid = nodeStore.getPropertyKeyId(key)
    if (pid.isDefined){
      nodeStore.nodeRemoveProperty(id, pid.get)
      val node = getNodeById(id).get
      if (node.properties.nonEmpty) indexStore.updateIndexOnSingleNode(node.longId, node.labels, node.properties.map(p => p._1->p._2.value), (true, key))
      else indexStore.deleteDoc(node.longId.toString)
    }
  }



  override def nodeAddLabel(id: Id, label: String): Unit = {
    val lid = nodeStore.addLabel(label)
    nodeStore.nodeAddLabel(id, lid)
    val node = getNodeById(id).get
    indexStore.updateIndexOnSingleNode(node.longId, node.labels, node.properties.map(p => p._1->p._2.value), (false, ""))

    statistics.increaseNodeLabelCount(lid, 1)
  }

  override def nodeRemoveLabel(id: Id, label: String): Unit = {
    val lid = nodeStore.getLabelId(label)
    lid.foreach(lid => nodeStore.nodeRemoveLabel(id, lid))
    val node = getNodeById(id).get
    if (node.labels.nonEmpty) indexStore.updateIndexOnSingleNode(node.longId, node.labels, node.properties.map(p => p._1->p._2.value), (false, ""))
    else indexStore.deleteDoc(node.longId.toString)

    statistics.decreaseNodeLabelCount(lid.get, 1)
  }


  override def addRelation(label: String, from: Id, to: Id, relProps: Map[String, Any]): Id = {
    addRelation(None, label, from, to, relProps)
  }

  override def addRelation(relId: Id, label: String, from: Id, to: Id, relProps: Map[String, Any]): Id = {
    addRelation(Option(relId), label, from, to, relProps)
  }

  private def addRelation(id: Option[Long], label: String, from: Long, to: Long, relProps: Map[String, Any]): Id = {
    val rid = id.getOrElse(relationStore.newRelationId())
    val labelId = relationStore.addRelationType(label)
    val props = relProps.map(v => (relationStore.addPropertyKey(v._1), v._2))
    val rel = new StoredRelationWithProperty(rid, from, to, labelId, props)
    relationStore.addRelation(rel)

    statistics.increaseRelationCount(1)
    statistics.increaseRelationTypeCount(labelId, 1)

    rid
  }

  override def scanAllRelations(): Iterator[PandaRelationship] = {
    relationStore.allRelations().map(mapRelation(_))
  }

  override def getRelation(id: Id): Option[PandaRelationship] = {
    relationStore.getRelationById(id).map(mapRelation(_))
  }

  override def getRelationTypeId(typeName: String): Option[Int] = relationStore.getRelationTypeId(typeName)

  override def transferInnerRelation(r: StoredRelation): PandaRelationship = mapRelation(r)

  protected def mapRelation(rel: StoredRelation): PandaRelationship = {
    PandaRelationship(rel.id,
      rel.from, rel.to,
      relationStore.getRelationTypeName(rel.typeId),
      rel.properties.map(kv => (relationStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq: _*)
  }

  override def deleteRelation(id: Id): Unit = {
    val relation = relationStore.getRelationById(id)
    if (relation.isDefined) {
      relationStore.deleteRelation(id)
      statistics.decreaseRelations(1)
      statistics.decreaseRelationLabelCount(relation.get.typeId, 1)
    }
  }

  override def deleteRelations(ids: Iterator[Id]): Unit = {
    ids.foreach(id => deleteRelation(id))
  }

  override def relationSetProperty(id: Id, key: String, value: Any): Unit = {
    relationStore.relationSetProperty(id, relationStore.addPropertyKey(key), value)
  }

  override def relationRemoveProperty(id: Id, key: String): Unit = {
    relationStore.getPropertyKeyId(key).foreach(kid => relationStore.relationRemoveProperty(id, kid))
  }

  override def relationAddType(id: Id, label: String): Unit = ???

  override def relationRemoveType(id: Id, label: String): Unit = ???

  override def findToNodeIds(fromNodeId: Id): Iterator[Id] = {
    relationStore.findToNodeIds(fromNodeId)
  }

  override def findToNodeIds(fromNodeId: Id, relationType: Int): Iterator[Id] = {
    relationStore.findToNodeIds(fromNodeId, relationType)
  }

  override def findFromNodeIds(toNodeId: Id): Iterator[Id] = {
    relationStore.findFromNodeIds(toNodeId)
  }

  override def findFromNodeIds(toNodeId: Id, relationType: Int): Iterator[Id] = {
    relationStore.findFromNodeIds(toNodeId, relationType)
  }

  override def findOutRelations(fromNodeId: Id): Iterator[StoredRelation] = {
    relationStore.findOutRelations(fromNodeId)
  }

  override def findOutRelations(fromNodeId: Id, edgeType: Option[Int]): Iterator[StoredRelation] = {
    relationStore.findOutRelations(fromNodeId, edgeType)
  }

  override def findInRelations(toNodeId: Id): Iterator[StoredRelation] = {
    relationStore.findInRelations(toNodeId)
  }

  override def findInRelations(toNodeId: Id, edgeType: Option[Int]): Iterator[StoredRelation] = {
    relationStore.findInRelations(toNodeId, edgeType)
  }

  override def createIndexOnNode(label: String, propNames: Set[String]): Unit = {
    val created = indexStore.isIndexCreated(label, propNames.toSeq)
    if (!created){
      val iter = getNodesByLabel(Seq(label), false)
      indexStore.batchAddIndexOnNodes(label, propNames.toSeq, iter)
    }
  }

  override def isNodeHasIndex(filter: NodeFilter): Boolean = {
    indexStore.isNodeHasIndex(filter)
  }

  override def getNodesByIndex(nodeFilter: NodeFilter): Iterator[PandaNode] ={
    indexStore.searchNodes(nodeFilter.labels, nodeFilter.properties.map(p => p._1->p._2.value)).flatten
  }

  override def cypher(query: String, parameters: Map[String, Any], tx: Option[LynxTransaction]): LynxResult = {
    runner.compile(query)
    runner.run(query, parameters, tx)
  }

  override def close(): Unit = {
    nodeStore.close()
    statistics.flush()
  }
}
