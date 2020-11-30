package cn.pandadb.kernel

import cn.pandadb.kernel.store.{NodeStore, RelationStore, StoredNode, StoredRelation}

trait GraphRAM extends NodeStore with RelationStore {
  type Id = Long

  def init(nodes: Iterator[StoredNode], rels: Iterator[StoredRelation]) = {
    nodes.foreach(addNode(_))
    rels.foreach(addRelation(_))
  }

  def relsMatchOneOf(relTypes: Set[String]): Iterator[StoredRelation] = ???

  def nodesMatchOneOf(labels: Set[String]): Iterator[StoredNode] = ???

  def addNode(t: StoredNode)

  def nodeAt(id: Id): StoredNode

  def relationAt(id: Id): StoredRelation

  def deleteNode(id: Id)

  def addRelation(t: StoredRelation)

  def deleteRelation(id: Id)

  def nodes(): Iterator[StoredNode]

  def rels(): Iterator[StoredRelation]

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