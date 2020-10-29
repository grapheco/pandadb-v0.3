package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

import scala.collection.mutable

class SimpleGraphRAM extends GraphRAM {
  val mapNodes = mutable.LinkedHashMap[Id, StoredNode]()
  val mapRelations = mutable.LinkedHashMap[Id, StoredRelation]()

  override def addNode(t: StoredNode): Unit = mapNodes += t.id -> t

  override def deleteNode(id: Id): Unit = mapNodes -= id

  override def addRelation(t: StoredRelation): Unit = mapRelations += t.id -> t

  override def deleteRelation(id: Id): Unit = mapRelations -= id

  override def nodes(): Seq[StoredNode] = mapNodes.map(_._2).toSeq

  override def rels(): Seq[StoredRelation] = mapRelations.map(_._2).toSeq

  override def close(): Unit = {
    clear()
  }

  override def clear(): Unit = {
    mapNodes.clear()
    mapRelations.clear()
  }

  override def nodeAt(id: Id): StoredNode = mapNodes(id)

  override def relationAt(id: Id): StoredRelation = mapRelations(id)
}

