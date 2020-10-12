package cn.pandadb.pnode

import cn.pandadb.pnode.store.{MergedGraphLogs, _}
import org.apache.logging.log4j.scala.Logging

class GraphFacade(nodeStore: FileBasedNodeStore,
                  relStore: FileBasedRelationStore,
                  logStore: FileBasedLogStore,
                  nodeLabelStore: FileBasedLabelStore,
                  relLabelStore: FileBasedLabelStore,
                  nodeIdGen: FileBasedIdGen,
                  relIdGen: FileBasedIdGen,
                  mem: GraphRAM,
                  pop: PropertiesOp,
                  onClose: => Unit) extends Logging {
  def close(): Unit = {
    nodeStore.close
    relStore.close
    logStore.close
    nodeIdGen.flush()
    relIdGen.flush()
    mem.close
    pop.close

    onClose
  }

  val thread = new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        Thread.sleep(600000)
        if (logStore.length > 102400) {
          logger.debug(s"starting log merging...")
          mergeLogs2Store(true)
          logger.debug(s"completed log merging...")
        }
      }
    }
  })

  def mergeLogs2Store(updateMem: Boolean): Unit = {
    logStore.offer {
      (logs: MergedGraphLogs) =>
        //mem should be appended before creating logs
        nodeStore.append(logs.nodes.toAdd)

        nodeStore.replace(logs.nodes.toReplace)
        if (updateMem) {
          logs.nodes.toReplace.foreach { x =>
            mem.updateNodePosition(x._2, x._1)
          }
        }

        nodeStore.remove(logs.nodes.toDelete, (pos1: Long, pos2: Long) => {
          if (updateMem) {
            mem.updateNodePosition(pos1, pos2)
          }
        })

        relStore.append(logs.rels.toAdd)
        relStore.replace(logs.rels.toReplace)
        relStore.remove(logs.rels.toDelete, (pos1: Long, pos2: Long) => {
          if (updateMem) {
            mem.updateRelationPosition(pos1, pos2)
          }
        })
    }
  }

  //FIXME: expensive time cost
  def init(): Unit = {
    mergeLogs2Store(false)
    mem.init(nodeStore.loadAllWithPosition(), relStore.loadAllWithPosition())
    thread.start()
  }

  def addNode(props: Map[String, Any], labels: String*): this.type = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = (Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0) ++ nodeLabelStore.ids(labels.toSet).zipWithIndex).values.toArray
    val node = StoredNode(nodeId, labelIds(0), labelIds(1), labelIds(2), labelIds(3))
    //TODO: transaction safe
    logStore.append(CreateNode(node))
    pop.create(NodeId(nodeId), props)
    mem.addNode(node)
    this
  }

  def addRelation(label: String, from: Long, to: Long, props: Map[String, Any]): this.type = {
    val rid = relIdGen.nextId()
    val labelId = relLabelStore.id(label)
    val rel = StoredRelation(rid, from, to, labelId)
    //TODO: transaction safe
    logStore.append(CreateRelation(rel))
    pop.create(RelationId(rid), props)
    mem.addRelation(rel)
    this
  }

  def deleteNode(id: Long): this.type = {
    logStore.append(DeleteNode(id, mem.nodePosition(id).getOrElse(-1)))
    pop.delete(NodeId(id))
    mem.deleteNode(id)
    this
  }

  def deleteRelation(id: Long): this.type = {
    logStore.append(DeleteRelation(id, mem.relationPosition(id).getOrElse(-1)))
    pop.delete(RelationId(id))
    mem.deleteRelation(id)
    this
  }

  def snapshot(): Unit = {
    //TODO: transaction safe
    nodeStore.saveAll(mem.nodes())
    relStore.saveAll(mem.rels())
    logStore.clear()
  }
}
