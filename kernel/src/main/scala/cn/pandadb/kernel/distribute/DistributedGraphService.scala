package cn.pandadb.kernel.distribute

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

  def nodeSetProperty(id: Id, key: String, value: Any): Unit

  def nodeRemoveProperty(id: Id, key: String): Unit

  def nodeAddLabel(id: Id, label: String): Unit

  def nodeRemoveLabel(id: Id, label: String): Unit

  def deleteNode(id: Id): Unit

  def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]): Id

  def relationSetProperty(id: Id, key: String, value: Any): Unit

  def relationRemoveProperty(id: Id, key: String): Unit

  def relationAddLabel(id: Id, label: String): Unit

  def relationRemoveLabel(id: Id, label: String): Unit

  def deleteRelation(id: Id): Unit

  def createIndexOnNode(label: String, props: Set[String]): Unit

  def cypher(query: String, parameters: Map[String, Any] = Map.empty, tx: Option[LynxTransaction] = None): LynxResult

  def close(): Unit
}
