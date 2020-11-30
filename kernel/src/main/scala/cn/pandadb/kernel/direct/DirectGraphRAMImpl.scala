package cn.pandadb.kernel.direct

import cn.pandadb.kernel.GraphRAM
import cn.pandadb.kernel.store.{StoredNode, StoredRelation}

import scala.collection.mutable

class DirectGraphRAMImpl extends GraphRAM {
  val relationshipStore = new OutEdgeRelationIndex()
  val mapRelations = mutable.LinkedHashMap[Id, StoredRelation]()

  val mapNodes = mutable.LinkedHashMap[Id, StoredNode]()

  override def addNode(t: StoredNode): Unit = mapNodes += t.id -> t

  override def deleteNode(id: Id): Unit = mapNodes -= id

  override def nodeAt(id: Id): StoredNode = mapNodes(id)

  override def relationAt(id: Id): StoredRelation = mapRelations(id)

  override def addRelation(t: StoredRelation): Unit = {
    relationshipStore.addRelationship(fromNode = t.from, toNode = t.to, label = t.labelId)
    mapRelations += t.id -> t
  }

  override def deleteRelation(id: Id): Unit = {
    val r = relationAt(id)
    if (r != null) {
      relationshipStore.deleteRelation(r.from, r.to, r.labelId)
      mapRelations -= id
    }
  }

  override def nodes(): Iterator[StoredNode] = mapNodes.map(_._2).iterator

  override def rels(): Iterator[StoredRelation] = mapRelations.map(_._2).iterator

  override def clear(): Unit = {
    // todo
  }

  override def close(): Unit = {
    // todo
  }
}

