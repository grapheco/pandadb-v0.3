package cn.pandadb.kernel.distribute

import cn.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.grapheco.lynx.{LynxResult, LynxTransaction}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 16:46
 */
trait DistributedGraphService {
  type Id = Long

  def addNode(nodeProps: Map[String, Any], labels: String*): Id

  def getNode(id: Id): Option[PandaNode]

  def getNode(id: Id, labelName: String): Option[PandaNode]

  def scanAllNode(): Iterator[PandaNode]

  def deleteNode(id: Id): Unit

  def nodeSetProperty(id: Id, key: String, value: Any): Unit

  def nodeRemoveProperty(id: Id, key: String): Unit

  def nodeAddLabel(id: Id, label: String): Unit

  def nodeRemoveLabel(id: Id, label: String): Unit

  def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]): Id

  def getRelation(id: Id): Option[PandaRelationship]

  def scanAllRelations(): Iterator[PandaRelationship]

  def deleteRelation(id: Id): Unit

  def relationSetProperty(id: Id, key: String, value: Any): Unit

  def relationRemoveProperty(id: Id, key: String): Unit

  def relationAddType(id: Id, label: String): Unit

  def relationRemoveType(id: Id, label: String): Unit

  def createIndexOnNode(label: String, props: Set[String]): Unit

  def cypher(query: String, parameters: Map[String, Any] = Map.empty, tx: Option[LynxTransaction] = None): LynxResult

  def close(): Unit
}
