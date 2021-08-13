package cn.pandadb.kernel

import org.grapheco.lynx.{LynxResult, LynxTransaction}

trait GraphService {
  type Id = Long

  def cypher(query: String, parameters: Map[String, Any] = Map.empty, tx: Option[LynxTransaction] = None): LynxResult

  def close(): Unit

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

  def createIndexOnRelation(typeName: String, props: Set[String]): Unit
}

trait TransactionGraphService {
  type Id = Long

  def cypher(query: String, parameters: Map[String, Any], tx: Option[LynxTransaction]): LynxResult

  def close(): Unit

  def addNode(tx: Option[LynxTransaction], nodeProps: Map[String, Any], labels: String*): Id

  def nodeSetProperty(tx: Option[LynxTransaction], id: Id, key: String, value: Any): Unit

  def nodeRemoveProperty(tx: Option[LynxTransaction], id: Id, key: String): Unit

  def nodeAddLabel(tx: Option[LynxTransaction], id: Id, label: String): Unit

  def nodeRemoveLabel(tx: Option[LynxTransaction], id: Id, label: String): Unit

  def deleteNode(tx: Option[LynxTransaction], id: Id): Unit

  def addRelation(tx: Option[LynxTransaction], label: String, from: Long, to: Long, relProps: Map[String, Any]): Id

  def relationSetProperty(tx: Option[LynxTransaction], id: Id, key: String, value: Any): Unit

  def relationRemoveProperty(tx: Option[LynxTransaction], id: Id, key: String): Unit

  def relationAddLabel(tx: Option[LynxTransaction], id: Id, label: String): Unit

  def relationRemoveLabel(tx: Option[LynxTransaction], id: Id, label: String): Unit

  def deleteRelation(tx: Option[LynxTransaction], id: Id): Unit

  def createIndexOnNode(tx: Option[LynxTransaction], label: String, props: Set[String]): Unit

  def createIndexOnRelation(tx: Option[LynxTransaction], typeName: String, props: Set[String]): Unit
}