package org.grapheco.pandadb.kernel.distribute

import org.grapheco.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.grapheco.lynx.{LynxResult, NodeFilter}
import org.grapheco.pandadb.kernel.distribute.index.IndexStoreService
import org.grapheco.pandadb.kernel.distribute.meta.DistributedStatistics
import org.tikv.common.util.ScanOption
import org.tikv.kvproto.Kvrpcpb

import java.util

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 16:46
 */
trait DistributedGraphService {
  type Id = Long

  def cleanDB: Unit
  def getStatistics: DistributedStatistics
  def getIndexStore: IndexStoreService
  def cypher(query: String, parameters: Map[String, Any] = Map.empty): LynxResult
  def refreshMeta(): Unit // udp
  def close(): Unit

  // generate id
  def newNodeId(): Id
  def newRelationshipId(): Id

  // meta functions
  def getNodeLabelId(labelName: String): Option[Int]
  def getNodeLabelName(labelId: Int): Option[String]
  def getRelationTypeId(typeName: String): Option[Int]
  def getRelationTypeName(typeId: Int): Option[String]
  def getPropertyName(propertyId: Int): Option[String]
  def getPropertyId(propertyName: String): Option[Int]

  // index functions
  def createIndexOnNode(label: String, props: Set[String]): Unit
  def dropIndexOnNode(label: String, prop: String): Unit
  def getNodesByIndex(nodeFilter: NodeFilter): Iterator[PandaNode]

  // node
  def addNode(nodeProps: Map[String, Any], labels: String*): Id
  def addNode(nodeId: Long, nodeProps: Map[String, Any], labels: String*): Id
  def addNodes(nodes: Iterator[PandaNode]): Unit
  def deleteNode(id: Id): Unit
  def deleteNodes(ids: Iterator[Id])
  def nodeSetProperty(id: Id, key: String, value: Any): Unit
  def nodeRemoveProperty(id: Id, key: String): Unit
  def nodeAddLabel(id: Id, label: String): Unit
  def nodeRemoveLabel(id: Id, label: String): Unit
  def getNodeById(id: Id): Option[PandaNode]
  def getNodeById(id: Id, labelName: String): Option[PandaNode]
  def getNodesByIds(ids: Seq[Id], labelName: String): Seq[PandaNode]
  def getNodesByIds(ids: Seq[Id], labelId: Int): Seq[PandaNode]
  def getNodesByLabel(labelName: String, exact: Boolean): Iterator[PandaNode]
  def scanAllNodes(): Iterator[PandaNode]

  // relation
  def addRelation(typeName: String, from: Long, to: Long, relProps: Map[String, Any]): Id
  def addRelation(relId: Long, typeName: String, from: Long, to: Long, relProps: Map[String, Any]): Id
  def addRelations(relationships: Iterator[PandaRelationship]): Unit
  def deleteRelation(id: Id): Unit
  def deleteRelations(ids: Iterator[Id]): Unit
  def relationSetProperty(id: Id, key: String, value: Any): Unit
  def relationRemoveProperty(id: Id, key: String): Unit
  def relationAddType(id: Id, label: String): Unit
  def relationRemoveType(id: Id, label: String): Unit
  def getRelationById(id: Id): Option[PandaRelationship]
  def scanAllRelations(): Iterator[PandaRelationship]
  //  def findToNodeIds(fromNodeId: Long): Iterator[Long];
  //  def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long];
  //  def findFromNodeIds(toNodeId: Long): Iterator[Long];
  //  def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long];
  def findOutRelations(fromNodeId: Long): Iterator[PandaRelationship] = findOutRelations(fromNodeId, None)
  def findOutRelations(fromNodeId: Long, edgeType: Option[Int]): Iterator[PandaRelationship]
  def findInRelations(toNodeId: Long): Iterator[PandaRelationship] = findInRelations(toNodeId, None)
  def findInRelations(toNodeId: Long, edgeType: Option[Int]): Iterator[PandaRelationship]

  // ====================================== new added ======================================
  def countOutRelations(fromNodeId: Long): Long
  def countOutRelations(fromNodeId: Long, edgeType: Int): Long
  def findOutRelationsEndNodeIds(fromNodeId: Long): Iterator[Long]
  def findOutRelationsEndNodeIds(fromNodeId: Long, edgeType: Int): Iterator[Long]

  //origin
  def batchScan(list: util.List[ScanOption]): util.List[util.List[Kvrpcpb.KvPair]]
  // =======================================================================================
  def fullText(columnNames: Seq[String], text: String): Iterator[Long]
}
