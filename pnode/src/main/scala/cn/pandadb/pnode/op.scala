package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

trait GraphOp {
  def addNode(t: StoredNode)

  def deleteNode(id: Long)

  def addRelation(t: StoredRelation)

  def deleteRelation(id: Long)

  def addNodes(ts: Stream[StoredNode])

  def addRelations(ts: Stream[StoredRelation])

  def nodes(): Stream[StoredNode]

  def rels(): Stream[StoredRelation]

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