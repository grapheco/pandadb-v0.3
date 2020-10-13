package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

trait GraphRAM {
  type Id = Long
  type Position = Long

  def updateNodePosition(id: Id, pos2: Position)

  def updateRelationPosition(id: Id, pos2: Position)

  def addNode(t: StoredNode)

  def deleteNode(id: Id)

  def addRelation(t: StoredRelation)

  def deleteRelation(id: Id)

  def init(nodes: Stream[(Position, StoredNode)], rels: Stream[(Position, StoredRelation)])

  def nodes(): Stream[StoredNode]

  def rels(): Stream[StoredRelation]

  def clear(): Unit

  def nodePosition(id: Id): Option[Position]

  def relationPosition(id: Id): Option[Position]

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