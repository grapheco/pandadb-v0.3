package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

import scala.collection.mutable

class GraphRAMImpl extends GraphRAM {
  val mapNodes = mutable.LinkedHashMap[Long, StoredNode]()
  val mapRels = mutable.LinkedHashMap[Long, StoredRelation]()

  override def addNode(t: StoredNode): Unit = mapNodes += t.id -> t

  override def deleteNode(id: Long): Unit = mapNodes -= id

  override def addRelation(t: StoredRelation): Unit = mapRels += t.id -> t

  override def deleteRelation(id: Long): Unit = mapRels -= id

  override def nodes(): Stream[StoredNode] = mapNodes.values.toStream

  override def rels(): Stream[StoredRelation] = mapRels.values.toStream

  override def close(): Unit = {
    clear()
  }

  override def clear(): Unit = {
    mapNodes.clear()
    mapRels.clear()
  }

  override def updateNodePosition(pos1: Long, pos2: Long): Unit = ???

  override def updateRelationPosition(pos1: Long, pos2: Long): Unit = ???

  override def updateNodePosition(t: StoredNode, pos2: Long): Unit = ???

  override def updateRelationPosition(t: StoredRelation, pos2: Long): Unit = ???

  override def init(nodes: Stream[(Long, StoredNode)], rels: Stream[(Long, StoredRelation)]): Unit = ???

  override def nodePosition(id: Long): Option[Long] = ???

  override def relationPosition(id: Long): Option[Long] = ???
}

