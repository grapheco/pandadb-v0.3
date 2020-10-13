package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

import scala.collection.mutable

class GraphRAMImpl extends GraphRAM {
  val mapNodes = mutable.LinkedHashMap[Id, (StoredNode, Position)]()
  val mapRelations = mutable.LinkedHashMap[Id, (StoredRelation, Position)]()

  override def addNode(t: StoredNode): Unit = mapNodes += t.id -> (t, -1)

  override def deleteNode(id: Id): Unit = mapNodes -= id

  override def addRelation(t: StoredRelation): Unit = mapRelations += t.id -> (t, -1)

  override def deleteRelation(id: Id): Unit = mapRelations -= id

  override def nodes(): Stream[StoredNode] = mapNodes.map(_._2._1).toStream

  override def rels(): Stream[StoredRelation] = mapRelations.map(_._2._1).toStream

  override def close(): Unit = {
    clear()
  }

  override def clear(): Unit = {
    mapNodes.clear()
    mapRelations.clear()
  }

  private def nodeAt(pos: Position) = mapNodes.find(_._2._2 == pos).head._1

  private def relationAt(pos: Position) = mapRelations.find(_._2._2 == pos).head._1

  override def updateNodePosition(id: Id, pos2: Position): Unit = mapNodes(id) = {
    val t = mapNodes(id)
    t._1 -> pos2
  }

  override def updateRelationPosition(id: Id, pos2: Position): Unit = mapRelations(id) = {
    val t = mapRelations(id)
    t._1 -> pos2
  }

  override def init(nodes: Stream[(Position, StoredNode)], rels: Stream[(Position, StoredRelation)]): Unit = {
    clear()
    mapNodes ++= nodes.map((x) => x._2.id -> (x._2, x._1))
    mapRelations ++= rels.map((x) => x._2.id -> (x._2, x._1))
  }

  override def nodePosition(id: Id): Option[Long] = Some(mapNodes(id)._2).filter(_ >= 0)

  override def relationPosition(id: Id): Option[Long] = Some(mapRelations(id)._2).filter(_ >= 0)
}

