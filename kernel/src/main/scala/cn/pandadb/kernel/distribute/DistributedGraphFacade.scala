package cn.pandadb.kernel.distribute
import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import cn.pandadb.kernel.distribute.node.NodeStoreAPI
import cn.pandadb.kernel.store.{PandaNode, PandaRelationship, StoredNode, StoredNodeWithProperty}
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.grapheco.lynx.{LynxResult, LynxTransaction, LynxValue}
import org.tikv.common.{TiConfiguration, TiSession}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 16:45
 */
class DistributedGraphFacade extends DistributedGraphService{
  val nodeIndex = "node-meta"
  val relationIndex = "relation-meta"
  val propertyIndex = "property-meta"

  val indexStore = {
    val hosts = Array(new HttpHost("10.0.82.144", 9200, "http"),
      new HttpHost("10.0.82.145", 9200, "http"),
      new HttpHost("10.0.82.146", 9200, "http"))
    new PandaDistributedIndexStore(new RestHighLevelClient(RestClient.builder(hosts: _*)))
  }

  val db = {
    val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
    val session = TiSession.create(conf)
    new PandaDistributeKVAPI(session.createRawClient())
  }
  val nodeStore = new NodeStoreAPI(db, indexStore)

  override def addNode(nodeProps: Map[String, Any], labels: String*): Id = {
    addNode(None, labels, nodeProps)
  }
  private def addNode(id: Option[Long], labels: Seq[String], nodeProps: Map[String, Any]): Id = {
    val nodeId = id.getOrElse(nodeStore.newNodeId())
    val labelIds = labels.map(label => nodeStore.addLabel(label)).toArray
    val properties =  nodeProps.map(kv => (nodeStore.addPropertyKey(kv._1), kv._2))
    nodeStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, properties))

    //TODO: if label && property has index?

    nodeId
  }

  override def getNode(id: Id): Option[PandaNode] = {
    nodeStore.getNodeById(id).map(mapNode(_))
  }

  override def getNode(id: Id, labelName: String): Option[PandaNode] = {
    nodeStore.getNodeById(id, nodeStore.getLabelId(labelName)).map(mapNode(_))
  }

  override def scanAllNode(): Iterator[PandaNode] = {
    nodeStore.allNodes().map(mapNode(_))
  }

  override def deleteNode(id: Id): Unit = {
    nodeStore.deleteNode(id)
  }

  protected def mapNode(node: StoredNode): PandaNode = {
    PandaNode(node.id,
      node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq,
      node.properties.map(kv => (nodeStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq: _*)
  }

  override def nodeSetProperty(id: Id, key: String, value: Any): Unit = {
    nodeStore.nodeSetProperty(id, nodeStore.addPropertyKey(key), value)
  }

  override def nodeRemoveProperty(id: Id, key: String): Unit = {
    nodeStore.getPropertyKeyId(key).foreach(pid => nodeStore.nodeRemoveProperty(id, pid))
  }

  override def nodeAddLabel(id: Id, label: String): Unit = {
    nodeStore.nodeAddLabel(id, nodeStore.addLabel(label))
  }

  override def nodeRemoveLabel(id: Id, label: String): Unit = {
    nodeStore.getLabelId(label).foreach(lid => nodeStore.nodeRemoveLabel(id, lid))
  }


  override def addRelation(label: String, from: Id, to: Id, relProps: Map[String, Any]): Id = ???

  override def getRelation(id: Id): Option[PandaRelationship] = ???

  override def getRelation(id: Id, typeName: String): Option[PandaRelationship] = ???

  override def deleteRelation(id: Id): Unit = ???

  override def relationSetProperty(id: Id, key: String, value: Any): Unit = ???

  override def relationRemoveProperty(id: Id, key: String): Unit = ???

  override def relationAddLabel(id: Id, label: String): Unit = ???

  override def relationRemoveLabel(id: Id, label: String): Unit = ???

  override def createIndexOnNode(label: String, props: Set[String]): Unit = ???

  override def cypher(query: String, parameters: Map[String, Any], tx: Option[LynxTransaction]): LynxResult = ???

  override def close(): Unit = {
    nodeStore.close()
  }
}
