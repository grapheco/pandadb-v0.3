package cn.pandadb.kernel

//import cn.pandadb.kernel.kv.StoredRelation
import cn.pandadb.kernel.store.{MergedGraphChanges, _}
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
                 ) extends Logging with GraphService {

  private val propertyGraph = new LynxSession().createPropertyGraph(new PropertyGraphScan[Long] {
    private def mapRelation(rel: StoredRelation): Relationship[Id] = {
      new Relationship[Id] {
        override type I = this.type

        override def id: Id = rel.id

        override def startId: Id = rel.from

        override def endId: Id = rel.to

        override def relType: String = relLabelStore.key(rel.typeId).get

        override def copy(id: Id, source: Id, target: Id, relType: String, properties: CypherMap): this.type = ???

        override def properties: CypherMap = CypherMap(props.lookup(RelationId(rel.id)).get.toSeq: _*)
      }
    }

    private def mapNode(node: StoredNode): Node[Id] = {
      new Node[Id] {
        override type I = this.type

        override def id: Id = node.id

        override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeLabelStore.key(id).get)

        override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

        override def properties: CypherValue.CypherMap = CypherMap(props.lookup(NodeId(node.id)).get.toSeq: _*)
      }
    }

    override def nodeAt(id: Long): CypherValue.Node[Long] = mapNode(mem.nodeAt(id))

    override def allNodes(labels: Set[String], exactLabelMatch: Boolean): Iterable[Node[Id]] = mem.nodesMatchOneOf(labels).map { node =>
      mapNode(node)
    }.toIterable

    override def allNodes(): Iterable[Node[Id]] = mem.nodes().map { node =>
      mapNode(node)
    }.toIterable

    override def allRelationships(): Iterable[CypherValue.Relationship[Id]] = mem.rels().map { rel =>
      mapRelation(rel)
    }.toIterable

    override def allRelationships(relTypes: Set[String]): Iterable[Relationship[Id]] = mem.relsMatchOneOf(relTypes).map { rel =>
      mapRelation(rel)
    }.toIterable
  })

  val thread = new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        Thread.sleep(600000)
        if (logStore.length > 102400) {
          logger.debug(s"starting log merging...")
          mergeLogs2Store()
          logger.debug(s"completed log merging...")
        }
      }
    }
  })

  init()

  override def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = {
    propertyGraph.cypher(query, CypherMap(parameters.toSeq: _*))
  }

  override def close(): Unit = {
    //nodeStore.close
    //relStore.close
    logStore.close
    nodeIdGen.flush()
    relIdGen.flush()
    mem.close
    props.close

    onClose
  }

  override def addNode(nodeProps: Map[String, Any], labels: String*): this.type = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = nodeLabelStore.ids(labels.toSet).toArray
    val node = StoredNode(nodeId, labelIds)
    //TODO: transaction safe
    logStore.append(CreateNode(node))
    props.insert(NodeId(nodeId), nodeProps)
    mem.addNode(node)
    this
  }

  override def addRelation(label: String, from: Long, to: Long, category: Int, relProps: Map[String, Any]): this.type = {
    val rid = relIdGen.nextId()
    val labelId = relLabelStore.id(label)
    val rel = StoredRelation(rid, from, to, labelId, category)
    //TODO: transaction safe
    logStore.append(CreateRelation(rel))
    props.insert(RelationId(rid), relProps)
    mem.addRelation(rel)
    this
  }

  override def deleteNode(id: Id): this.type = {
    logStore.append(DeleteNode(id))
    props.delete(NodeId(id))
    mem.deleteNode(id)
    this
  }

  override def deleteRelation(id: Id): this.type = {
    logStore.append(DeleteRelation(id))
    props.delete(RelationId(id))
    mem.deleteRelation(id)
    this
  }

  def mergeLogs2Store(): Unit = {
    logStore.offer {
      (logs: MergedGraphChanges) =>
        //mem should be appended before creating logs
        //nodeStore.merge(logs.nodes)
        //relStore.merge(logs.rels)
    }
  }

  //FIXME: expensive time cost
  private def init(): Unit = {
    mergeLogs2Store()
    mem.clear()
    //mem.init(nodeStore.loadAll(), relStore.loadAll())
    thread.start()
  }

  def snapshot(): Unit = {
    //TODO: transaction safe
    //nodeStore.saveAll(mem.nodes())
    //relStore.saveAll(mem.rels())
    logStore.clear()
  }
}
