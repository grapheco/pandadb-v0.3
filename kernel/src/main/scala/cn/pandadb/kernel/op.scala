package cn.pandadb.kernel

import cn.pandadb.kernel.store.{StoredNode, StoredRelation}

trait GraphRAM {
  type Id = Long

  def addNode(t: StoredNode)

  def nodeAt(id: Id): StoredNode

  def relationAt(id: Id): StoredRelation

  def deleteNode(id: Id)

  def addRelation(t: StoredRelation)

  def deleteRelation(id: Id)

  def nodes(): Seq[StoredNode]

  def rels(): Seq[StoredRelation]

  def clear(): Unit

  def close(): Unit
}

trait TypedId

case class NodeId(id: Long) extends TypedId

case class RelationId(id: Long) extends TypedId

trait PropertyStore {
  def insert(id: TypedId, props: Map[String, Any])

  def delete(id: TypedId)

  def lookup(id: TypedId): Option[Map[String, Any]]

  def close()
}