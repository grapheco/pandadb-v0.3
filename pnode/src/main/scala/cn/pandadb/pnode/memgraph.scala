package cn.pandadb.pnode

import cn.pandadb.pnode.store.{Node, Relation}

import scala.collection.mutable

class MemGraphOp extends GraphOp {
  val mapNodes = mutable.LinkedHashMap[Long, Node]()
  val mapRels = mutable.LinkedHashMap[Long, Relation]()

  def addNodes(ts: Stream[Node]): Unit = {
    mapNodes ++= ts.map(x => x.id -> x)
  }

  def addRelations(ts: Stream[Relation]): Unit = {
    mapRels ++= ts.map(x => x.id -> x)
  }

  override def addNode(t: Node): Unit = mapNodes += t.id -> t

  override def deleteNode(id: Long): Unit = mapNodes -= id

  override def addRelation(t: Relation): Unit = mapRels += t.id -> t

  override def deleteRelation(id: Long): Unit = mapRels -= id

  override def nodes(): Stream[Node] = mapNodes.values.toStream

  override def rels(): Stream[Relation] = mapRels.values.toStream

  override def close(): Unit = {
    mapNodes.clear()
    mapRels.clear()
  }
}

