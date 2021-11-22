package cn.pandadb.kernel.distribute

import cn.pandadb.kernel.store.{PandaNode, PandaRelationship, StoredNode, StoredRelation}
import org.grapheco.lynx.{LynxResult, LynxTransaction}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 16:46
 */
trait DistributedGraphService {
  type Id = Long

  def newNodeId(): Id

  def newRelationshipId(): Id

  def addNode(nodeProps: Map[String, Any], labels: String*): Id

  def addNode(nodeId: Long, nodeProps: Map[String, Any], labels: String*): Id

  def getNode(id: Id): Option[PandaNode]

  def getNode(id: Id, labelName: String): Option[PandaNode]

  def getNodesByLabel(labelNames: Seq[String], exact: Boolean): Iterator[PandaNode]

  def getNodeLabelId(labelName: String): Option[Int]

  def transferInnerNode(n: StoredNode): PandaNode

  def scanAllNode(): Iterator[PandaNode]

  def deleteNode(id: Id): Unit

  def deleteNodes(ids: Iterator[Id])

  def nodeSetProperty(id: Id, key: String, value: Any): Unit

  def nodeRemoveProperty(id: Id, key: String): Unit

  def nodeAddLabel(id: Id, label: String): Unit

  def nodeRemoveLabel(id: Id, label: String): Unit

  def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]): Id

  def addRelation(relId: Long, label: String, from: Long, to: Long, relProps: Map[String, Any]): Id

  def getRelation(id: Id): Option[PandaRelationship]

  def getRelationTypeId(typeName: String): Option[Int]

  def transferInnerRelation(r: StoredRelation): PandaRelationship

  def scanAllRelations(): Iterator[PandaRelationship]

  def deleteRelation(id: Id): Unit

  def deleteRelations(ids: Iterator[Id]): Unit

  def relationSetProperty(id: Id, key: String, value: Any): Unit

  def relationRemoveProperty(id: Id, key: String): Unit

  def relationAddType(id: Id, label: String): Unit

  def relationRemoveType(id: Id, label: String): Unit

  def findToNodeIds(fromNodeId: Long): Iterator[Long];

  def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long];

  def findFromNodeIds(toNodeId: Long): Iterator[Long];

  def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long];

  def findOutRelations(fromNodeId: Long): Iterator[StoredRelation] = findOutRelations(fromNodeId, None)

  def findOutRelations(fromNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation]

  def findInRelations(toNodeId: Long): Iterator[StoredRelation] = findInRelations(toNodeId, None)

  def findInRelations(toNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation]

  def createIndexOnNode(label: String, props: Set[String]): Unit

  def cypher(query: String, parameters: Map[String, Any] = Map.empty, tx: Option[LynxTransaction] = None): LynxResult

  def close(): Unit
}
