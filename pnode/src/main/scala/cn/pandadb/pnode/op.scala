package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

trait GraphRAM {
  type Id = Long

  def addNode(t: StoredNode)

  def nodeAt(id: Id): StoredNode

  def relationAt(id: Id): StoredRelation

  def deleteNode(id: Id)

  def addRelation(t: StoredRelation)

  def deleteRelation(id: Id)

  def nodes(): Stream[StoredNode]

  def rels(): Stream[StoredRelation]

  def clear(): Unit

  def close(): Unit
}

trait Properties {
  def add(key: String, value: Any)

  def delete(key: String)

  def set(key: String, value: Any)
}

trait TypedId

case class NodeId(id: Long) extends TypedId

case class RelationId(id: Long) extends TypedId

trait PropertiesOp {
  def create(id: TypedId, props: Map[String, Any])

  def delete(id: TypedId)

  def lookup(id: TypedId): Option[Map[String, Any]]

  def close()
}