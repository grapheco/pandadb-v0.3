package cn.pandadb.pnode

import cn.pandadb.pnode.store.{Node, Relation}

trait GraphOp {
  def addNode(t: Node)

  def deleteNode(id: Long)

  def addRelation(t: Relation)

  def deleteRelation(id: Long)

  def addNodes(ts: Stream[Node])

  def addRelations(ts: Stream[Relation])

  def nodes(): Stream[Node]

  def rels(): Stream[Relation]

  def close()
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