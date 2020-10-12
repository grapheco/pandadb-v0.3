package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

trait GraphRAM {
  def updateNodePosition(pos1: Long, pos2: Long)

  def updateRelationPosition(pos1: Long, pos2: Long)

  def updateNodePosition(t: StoredNode, pos2: Long)

  def updateRelationPosition(t: StoredRelation, pos2: Long)

  def addNode(t: StoredNode)

  def deleteNode(id: Long)

  def addRelation(t: StoredRelation)

  def deleteRelation(id: Long)

  def init(nodes: Stream[(Long, StoredNode)], rels: Stream[(Long, StoredRelation)])

  def nodes(): Stream[StoredNode]

  def rels(): Stream[StoredRelation]

  def clear(): Unit

  def nodePosition(id: Long): Option[Long]

  def relationPosition(id: Long): Option[Long]

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