package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

import scala.collection.mutable

class GraphRAMImpl extends GraphRAM {
  val mapNodes = mutable.LinkedHashMap[Id, StoredNode]()
  val mapRelations = mutable.LinkedHashMap[Id, StoredRelation]()
  val mapNodePositions = mutable.LinkedHashMap[Id, Position]()
  val mapRelationPositions = mutable.LinkedHashMap[Id, Position]()

  override def addNode(t: StoredNode): Unit = mapNodes += t.id -> t

  override def deleteNode(id: Id): Unit = {
    mapNodes -= id
    mapNodePositions -= id
  }

  override def addRelation(t: StoredRelation): Unit = mapRelations += t.id -> t

  override def deleteRelation(id: Id): Unit = {
    mapRelations -= id
    mapRelationPositions -= id
  }

  override def nodes(): Stream[StoredNode] = mapNodes.map(_._2).toStream

  override def rels(): Stream[StoredRelation] = mapRelations.map(_._2).toStream

  override def close(): Unit = {
    clear()
  }

  override def clear(): Unit = {
    mapNodes.clear()
    mapRelations.clear()
  }

  private def nodeAt(pos: Position) = mapNodePositions.find(_._2 == pos).head._1

  private def relationAt(pos: Position) = mapRelationPositions.find(_._2 == pos).head._1

  override def updateNodePosition(t: StoredNode, pos2: Position): Unit = mapNodePositions(t.id) = pos2

  override def updateRelationPosition(t: StoredRelation, pos2: Position): Unit = mapRelationPositions(t.id) = pos2

  override def init(nodes: Stream[(Position, StoredNode)], rels: Stream[(Position, StoredRelation)]): Unit = {
    clear()
    nodes.foreach((x) => {
      mapNodes(x._2.id) = x._2
      mapNodePositions(x._2.id) = x._1
    })

    rels.foreach((x) => {
      mapRelations(x._2.id) = x._2
      mapRelationPositions(x._2.id) = x._1
    })
  }

  override def nodePosition(id: Id): Option[Long] = mapNodePositions.get(id)

  override def relationPosition(id: Id): Option[Long] = mapRelationPositions.get(id)
}

