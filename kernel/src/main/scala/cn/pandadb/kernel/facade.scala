package cn.pandadb.kernel

import cn.pandadb.kernel.store.{MergedGraphLogs, _}
import org.apache.logging.log4j.scala.Logging
import org.opencypher.lynx.{LynxSession, PropertyGraphScan}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

class GraphFacade(
                   nodeStore: NodeStore,
                   relStore: RelationStore,
                   logStore: LogStore,
                   nodeLabelStore: LabelStore,
                   relLabelStore: LabelStore,
                   nodeIdGen: FileBasedIdGen,
                   relIdGen: FileBasedIdGen,
                   mem: GraphRAM,
                   props: PropertyStore,
                   onClose: => Unit
                 ) extends Logging {

  type Id = Long
  type Position = Long

  private val graphService = new LynxSession().createPropertyGraph(new PropertyGraphScan[Long] {
    def mapNode(node: StoredNode): Node[Id] = {
      new Node[Id] {
        override type I = this.type

        override def id: Id = node.id

        override def labels: Set[String] = Set(node.labelId1, node.labelId2, node.labelId3, node.labelId4).filter(_ > 0).map(nodeLabelStore.key(_).get)

        override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

        override def properties: CypherValue.CypherMap = CypherMap(props.lookup(NodeId(node.id)).get.toSeq: _*)
      }
    }

    override def nodeAt(id: Long): CypherValue.Node[Long] = mapNode(mem.nodeAt(id))

    override def allNodes(): Seq[Node[Id]] = mem.nodes().map { node =>
      mapNode(node)
    }

    override def allRelationships(): Seq[CypherValue.Relationship[Id]] = mem.rels().map { rel =>
      new Relationship[Id] {
        override type I = this.type

        override def id: Id = rel.id

        override def startId: Id = rel.from

        override def endId: Id = rel.to

        override def relType: String = relLabelStore.key(rel.labelId).get

        override def copy(id: Id, source: Id, target: Id, relType: String, properties: CypherMap): this.type = ???

        override def properties: CypherMap = CypherMap(props.lookup(RelationId(rel.id)).get.toSeq: _*)
      }
    }
  })

  def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = {
    graphService.cypher(query, CypherMap(parameters.toSeq: _*))
  }

  def close(): Unit = {
    nodeStore.close
    relStore.close
    logStore.close
    nodeIdGen.flush()
    relIdGen.flush()
    mem.close
    props.close

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

        if (logs.nodes.toAdd.nonEmpty)
          nodeStore.updateAll(logs.nodes.toAdd)
        if (logs.nodes.toReplace.nonEmpty)
          nodeStore.updateAll(logs.nodes.toReplace.map(_._2))
        if (logs.nodes.toDelete.nonEmpty) {
          nodeStore.deleteAll(logs.nodes.toDelete)
        }

        if (logs.rels.toAdd.nonEmpty)
          relStore.updateAll(logs.rels.toAdd)
        if (logs.rels.toReplace.nonEmpty)
          relStore.updateAll(logs.rels.toReplace.map(_._2))
        if (logs.rels.toDelete.nonEmpty)
          relStore.deleteAll(logs.rels.toDelete)
    }
  }

  //FIXME: expensive time cost
  def init(): Unit = {
    mergeLogs2Store(false)
    mem.clear()
    mem.init(nodeStore.loadAll(), relStore.loadAll())
    thread.start()
  }

  def addNode(nodeProps: Map[String, Any], labels: String*): this.type = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = (Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0) ++ nodeLabelStore.ids(labels.toSet).zipWithIndex).values.toArray
    val node = StoredNode(nodeId, labelIds(0), labelIds(1), labelIds(2), labelIds(3))
    //TODO: transaction safe
    logStore.append(CreateNode(node))
    props.insert(NodeId(nodeId), nodeProps)
    mem.addNode(node)
    this
  }

  def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]): this.type = {
    val rid = relIdGen.nextId()
    val labelId = relLabelStore.id(label)
    val rel = StoredRelation(rid, from, to, labelId)
    //TODO: transaction safe
    logStore.append(CreateRelation(rel))
    props.insert(RelationId(rid), relProps)
    mem.addRelation(rel)
    this
  }

  def deleteNode(id: Id): this.type = {
    logStore.append(DeleteNode(id))
    props.delete(NodeId(id))
    mem.deleteNode(id)
    this
  }

  def deleteRelation(id: Id): this.type = {
    logStore.append(DeleteRelation(id))
    props.delete(RelationId(id))
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
