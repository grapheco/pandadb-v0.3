package cn.pandadb.pnode

import cn.pandadb.pnode.store.{MergedGraphLogs, _}
import org.apache.logging.log4j.scala.Logging

class GraphFacade(
                   nodeStore: NodeStore,
                   relStore: RelationStore,
                   logStore: LogStore,
                   nodeLabelStore: LabelStore,
                   relLabelStore: LabelStore,
                   nodeIdGen: FileBasedIdGen,
                   relIdGen: FileBasedIdGen,
                   mem: GraphRAM,
                   pop: PropertiesOp,
                   onClose: => Unit
                 ) extends Logging {

  type Id = Long
  type Position = Long

  val (posNodes, posRels) = (new SimplePositionMap(), new SimplePositionMap())

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
        val positNode = (t: StoredNode, pos: Position) => {
          if (updateMem) {
            posNodes.update(t.id, pos)
          }
        }

        if (logs.nodes.toAdd.nonEmpty)
          nodeStore.append(logs.nodes.toAdd, positNode)
        if (logs.nodes.toReplace.nonEmpty)
          nodeStore.overwrite(logs.nodes.toReplace, positNode)
        if (logs.nodes.toDelete.nonEmpty) {
          nodeStore.remove(logs.nodes.toDelete, positNode)
        }

        val positRelation = (t: StoredRelation, pos: Position) => {
          if (updateMem) {
            posRels.update(t.id, pos)
          }
        }

        if (logs.rels.toAdd.nonEmpty)
          relStore.append(logs.rels.toAdd, positRelation)
        if (logs.rels.toReplace.nonEmpty)
          relStore.overwrite(logs.rels.toReplace, positRelation)
        if (logs.rels.toDelete.nonEmpty)
          relStore.remove(logs.rels.toDelete, positRelation)
    }
  }

  //FIXME: expensive time cost
  def init(): Unit = {
    mergeLogs2Store(false)
    mem.clear()
    posNodes.clear()
    posRels.clear()
    nodeStore.loadAllWithPosition().foreach { tp =>
      mem.addNode(tp._2)
      posNodes.update(tp._2.id, tp._1)
    }

    relStore.loadAllWithPosition().foreach { tp =>
      mem.addRelation(tp._2)
      posRels.update(tp._2.id, tp._1)
    }

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

  def deleteNode(id: Id): this.type = {
    logStore.append(DeleteNode(id, posNodes.position(id).getOrElse(-1)))
    pop.delete(NodeId(id))
    mem.deleteNode(id)
    this
  }

  def deleteRelation(id: Id): this.type = {
    logStore.append(DeleteRelation(id, posRels.position(id).getOrElse(-1)))
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
