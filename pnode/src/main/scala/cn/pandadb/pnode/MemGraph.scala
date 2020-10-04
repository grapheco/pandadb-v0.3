package cn.pandadb.pnode

import cn.pandadb.pnode.store._

import scala.collection.mutable

class MemGraph(nodeStore: FileBasedNodeStore, relStore: FileBasedRelationStore, logStore: FileBasedLogStore) {

  val mapNodes = mutable.LinkedHashMap[Long, Node]()
  val mapRels = mutable.LinkedHashMap[Long, Relation]()

  def loadAll(): Unit = {
    mapNodes ++= nodeStore.list().map(x => x.id -> x)
    mapRels ++= relStore.list().map(x => x.id -> x)

    //load logs
    logStore.list().foreach {
      _ match {
        case CreateNode(t) =>
          mapNodes += t.id -> t
        case CreateRelation(t) =>
          mapRels += t.id -> t
        case DeleteNode(id) =>
          mapNodes -= id
        case DeleteRelation(id) =>
          mapRels -= id
      }
    }
  }

  def addNode(node: Node): this.type = {
    logStore.append(CreateNode(node))
    mapNodes += node.id -> node
    this
  }

  def addRelation(rel: Relation): this.type = {
    logStore.append(CreateRelation(rel))
    mapRels += rel.id -> rel
    this
  }

  def deleteNode(id: Long): this.type = {
    logStore.append(DeleteNode(id))
    mapNodes -= id
    this
  }

  def deleteRelation(id: Long): this.type = {
    logStore.append(DeleteRelation(id))
    mapRels -= id
    this
  }

  def dumpAll(): Unit = {
    //TODO: transaction safe
    nodeStore.save(mapNodes.values.toStream)
    relStore.save(mapRels.values.toStream)
    logStore.clear()
  }
}
