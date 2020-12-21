package cn.pandadb.kernel.kv

import scala.collection.mutable

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.store._
import org.apache.logging.log4j.scala.Logging
import org.opencypher.lynx.{LynxSession, PropertyGraphScan}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

class GraphFacade(
                   nodeLabelStore: LabelStore,
                   relLabelStore: LabelStore,
                   nodeIdGen: FileBasedIdGen,
                   relIdGen: FileBasedIdGen,
                   graphStore: RocksDBGraphAPI,
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

        //        override def properties: CypherMap = CypherMap(props.lookup(RelationId(rel.id)).get.toSeq: _*)
        override def properties: CypherMap = {
          var props: Map[String, Any] = null
          if (rel.isInstanceOf[StoredRelationWithProperty]) {
            props = rel.asInstanceOf[StoredRelationWithProperty].properties.asInstanceOf[Map[String, Any]]
          }
          CypherMap(props.toSeq: _*)
        }
      }
    }

    private def mapNode(node: StoredNode): Node[Id] = {
      new Node[Id] {
        override type I = this.type

        override def id: Id = node.id

        override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeLabelStore.key(id).get)

        override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

        override def properties: CypherMap = {
          var props: Map[String, Any] = null
          if (node.isInstanceOf[StoredNodeWithProperty_tobe_deprecated]) {
            props = node.asInstanceOf[StoredNodeWithProperty_tobe_deprecated].properties
          }
          else {
            val n = graphStore.nodeAt(node.id)
            props = n.asInstanceOf[StoredNodeWithProperty_tobe_deprecated].properties
          }
          CypherMap(props.toSeq: _*)
        }
      }
    }

    override def nodeAt(id: Long): CypherValue.Node[Long] = mapNode(
      graphStore.nodeAt(id)
    )

    override def allNodes(labels: Set[String], exactLabelMatch: Boolean): Iterable[Node[Id]] = {
      var nodes: Set[Id] = null
      val labelIds = nodeLabelStore.ids(labels)
      labelIds.foreach(labelId => {
        if (nodes == null) {
          nodes = graphStore.findNodes(labelId).toSet[Id]
        }
        else {
          if(exactLabelMatch) {  // intersect
            nodes = nodes & graphStore.findNodes(labelId).toSet[Id]
          }
          else {  // union
            nodes = nodes ++ graphStore.findNodes(labelId).toSet[Id]
          }
        }
      })
      nodes.map(nodeId => mapNode(graphStore.nodeAt(nodeId)))
    }


    override def allNodes(): Iterable[Node[Id]] = {
      graphStore.allNodes().map(node => mapNode(node)).toIterable
    }

    override def allRelationships(): Iterable[CypherValue.Relationship[Id]] = {
      graphStore.allRelations().map(rel => mapRelation(rel)).toIterable
    }

    override def allRelationships(relTypes: Set[String]): Iterable[Relationship[Id]] = {
      var relations: Set[Id] = Set[Id]()
      val labelIds = relLabelStore.ids(relTypes)
      labelIds.foreach(labelId => { // union
        relations = relations ++ graphStore.findRelations(labelId).toSet[Id]
      })

      relations.map(relId => mapRelation(graphStore.relationAt(relId)))

    }
  })


  init()

  override def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = {
    propertyGraph.cypher(query, CypherMap(parameters.toSeq: _*))
  }

  override def close(): Unit = {
    nodeIdGen.flush()
    relIdGen.flush()
    graphStore.close()
    onClose
  }

  override def addNode(nodeProps: Map[String, Any], labels: String*): this.type = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = nodeLabelStore.ids(labels.toSet).toArray
//    val node = StoredNode(nodeId, labelIds)
    //TODO: transaction safe
    graphStore.addNode(nodeId, labelIds, nodeProps )

    this
  }

  override def addRelation(label: String, from: Long, to: Long, category: Int, relProps: Map[String, Any]): this.type = {
    val rid = relIdGen.nextId()
    val labelId = relLabelStore.id(label)
//    val rel = StoredRelation(rid, from, to, labelId)
    //TODO: transaction safe
    graphStore.addRelation(rid, from, to, labelId, relProps)
    this
  }

  override def deleteNode(id: Id): this.type = {
    graphStore.deleteNode(id)
    this
  }

  override def deleteRelation(id: Id): this.type = {
    graphStore.deleteRelation(id)
    this
  }

  def allNodes(): Iterable[StoredNode] = {
    graphStore.allNodes().toIterable
  }

  def allRelations(): Iterable[StoredRelation] = {
    graphStore.allRelations().toIterable
  }

  //FIXME: expensive time cost
  private def init(): Unit = {

  }

  def snapshot(): Unit = {
    //TODO: transaction safe
  }
}
