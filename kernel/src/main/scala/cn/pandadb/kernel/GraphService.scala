package cn.pandadb.kernel

import org.grapheco.lynx.{LynxResult, LynxTransaction}

trait GraphService {
  type Id = Long

  def cypher(query: String, parameters: Map[String, Any] = Map.empty): LynxResult

  def cypher(query: String, parameters: Map[String, Any], tx: LynxTransaction): LynxResult

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

  def cypher(query: String, parameters: Map[String, Any], tx: LynxTransaction): LynxResult

  def close(): Unit

  def addNode(tx: LynxTransaction, nodeProps: Map[String, Any], labels: String*): Id

  def nodeSetProperty(tx: LynxTransaction, id: Id, key: String, value: Any): Unit

  def nodeRemoveProperty(tx: LynxTransaction, id: Id, key: String): Unit

  def nodeAddLabel(tx: LynxTransaction, id: Id, label: String): Unit

  def nodeRemoveLabel(tx: LynxTransaction, id: Id, label: String): Unit

  def deleteNode(tx: LynxTransaction, id: Id): Unit

  def addRelation(tx: LynxTransaction, label: String, from: Long, to: Long, relProps: Map[String, Any]): Id

  def relationSetProperty(tx: LynxTransaction, id: Id, key: String, value: Any): Unit

  def relationRemoveProperty(tx: LynxTransaction, id: Id, key: String): Unit

  def relationAddLabel(tx: LynxTransaction, id: Id, label: String): Unit

  def relationRemoveLabel(tx: LynxTransaction, id: Id, label: String): Unit

  def deleteRelation(tx: LynxTransaction, id: Id): Unit

  def createIndexOnNode(tx: LynxTransaction, label: String, props: Set[String]): Unit

  def createIndexOnRelation(tx: LynxTransaction, typeName: String, props: Set[String]): Unit
}