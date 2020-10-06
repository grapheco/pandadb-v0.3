package cn.pandadb.pnode

import cn.pandadb.pnode.store._

class GraphFacade(nodeStore: FileBasedNodeStore,
                  relStore: FileBasedRelationStore,
                  logStore: FileBasedLogStore,
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
    gop.addNodes(nodeStore.list())
    gop.addRelations(relStore.list())

    //load logs
    logStore.list().foreach {
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
    val nid = nodeIdGen.nextId()
    val node = Node(nid, labels: _*)
    //TODO: transaction safe
    logStore.append(CreateNode(node))
    pop.create(NodeId(nid), props)
    gop.addNode(node)
    this
  }

  def addRelation(label: String, from: Long, to: Long, props: Map[String, Any]): this.type = {
    val rid = relIdGen.nextId()
    val rel = Relation(rid, from, to, label)
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
