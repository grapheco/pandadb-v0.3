package cn.pandadb.pnode

import cn.pandadb.pnode.store._

import scala.collection.mutable

class GraphFacade(nodeStore: FileBasedNodeStore,
                  relStore: FileBasedRelationStore,
                  logStore: FileBasedLogStore,
                  nodeLabelStore: FileBasedLabelStore,
                  relLabelStore: FileBasedLabelStore,
                  nodeIdGen: FileBasedIdGen,
                  relIdGen: FileBasedIdGen,
                  gop: GraphOp,
                  pop: PropertiesOp,
                  onClose: => Unit) {
  def close(): Unit = {
    nodeStore.close
    relStore.close
    logStore.close
    nodeIdGen.flush()
    relIdGen.flush()
    gop.close
    pop.close

    onClose
  }

  //FIXME: expensive time cost
  def loadAll(): Unit = {
    gop.addNodes(nodeStore.load())
    gop.addRelations(relStore.load())

    //load logs
    logStore.load().foreach {
      _ match {
        case CreateNode(t) =>
          gop.addNode(t)
        case CreateRelation(t) =>
          gop.addRelation(t)
        case DeleteNode(id) =>
          gop.deleteNode(id)
        case DeleteRelation(id) =>
          gop.deleteRelation(id)
      }
    }
  }

  def addNode(props: Map[String, Any], labels: String*): this.type = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = (Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0) ++ nodeLabelStore.ids(labels.toSet).zipWithIndex).values.toArray
    val node = StoredNode(nodeId, labelIds(0), labelIds(1), labelIds(2), labelIds(3))
    //TODO: transaction safe
    logStore.append(CreateNode(node))
    pop.create(NodeId(nodeId), props)
    gop.addNode(node)
    this
  }

  def addRelation(label: String, from: Long, to: Long, props: Map[String, Any]): this.type = {
    val rid = relIdGen.nextId()
    val labelId = relLabelStore.id(label)
    val rel = StoredRelation(rid, from, to, labelId)
    //TODO: transaction safe
    logStore.append(CreateRelation(rel))
    pop.create(RelationId(rid), props)
    gop.addRelation(rel)
    this
  }

  def deleteNode(id: Long): this.type = {
    logStore.append(DeleteNode(id))
    pop.delete(NodeId(id))
    gop.deleteNode(id)
    this
  }

  def deleteRelation(id: Long): this.type = {
    logStore.append(DeleteRelation(id))
    pop.delete(RelationId(id))
    gop.deleteRelation(id)
    this
  }

  def dumpAll(): Unit = {
    //TODO: transaction safe
    nodeStore.save(gop.nodes())
    relStore.save(gop.rels())
    logStore.clear()
  }
}
