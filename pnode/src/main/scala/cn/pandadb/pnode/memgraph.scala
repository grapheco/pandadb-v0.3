package cn.pandadb.pnode

import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

import scala.collection.mutable

class MemGraphOp extends GraphOp {
  val mapNodes = mutable.LinkedHashMap[Long, StoredNode]()
  val mapRels = mutable.LinkedHashMap[Long, StoredRelation]()

  def addNodes(ts: Stream[StoredNode]): Unit = {
    mapNodes ++= ts.map(x => x.id -> x)
  }

  def addRelations(ts: Stream[StoredRelation]): Unit = {
    mapRels ++= ts.map(x => x.id -> x)
  }

  override def addNode(t: StoredNode): Unit = mapNodes += t.id -> t

  override def deleteNode(id: Long): Unit = mapNodes -= id

  override def addRelation(t: StoredRelation): Unit = mapRels += t.id -> t

  override def deleteRelation(id: Long): Unit = mapRels -= id

  override def nodes(): Stream[StoredNode] = mapNodes.values.toStream

  override def rels(): Stream[StoredRelation] = mapRels.values.toStream

  override def close(): Unit = {
    mapNodes.clear()
    mapRels.clear()
  }
}

