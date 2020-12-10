package cn.pandadb.kernel.kv

import cn.pandadb.kernel.optimizer.PandaPropertyGraphScan
import cn.pandadb.kernel.store.{FileBasedIdGen, LabelStore, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import org.opencypher.lynx.PropertyGraphScan
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

import scala.util.control._

class PropertyGraphScanImpl(nodeLabelStore: TokenStore,
                                relLabelStore: TokenStore,
                                nodeIdGen: FileBasedIdGen,
                                relIdGen: FileBasedIdGen,
                                graphStore: RocksDBGraphAPI) extends PropertyGraphScan[Long] {
  type Id = Long

  val loop = new Breaks

  private def mapRelation(rel: StoredRelation): Relationship[Id] = {
    new Relationship[Id] {
      override type I = this.type

      override def id: Id = rel.id

      override def startId: Id = rel.from

      override def endId: Id = rel.to

      override def relType: String = relLabelStore.key(rel.labelId).get

      override def copy(id: Id, source: Id, target: Id, relType: String, properties: CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = null
        if (rel.isInstanceOf[StoredRelationWithProperty]) {
          props = rel.asInstanceOf[StoredRelationWithProperty].properties
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  protected def mapNode(node: StoredNode): Node[Id] = {
    new Node[Id] {
      override type I = this.type

      override def id: Id = node.id

      override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeLabelStore.key(id).get)

      override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = null
        if (node.isInstanceOf[StoredNodeWithProperty]) {
          props = node.asInstanceOf[StoredNodeWithProperty].properties
        }
        else {
          val n = graphStore.nodeAt(node.id)
          props = n.asInstanceOf[StoredNodeWithProperty].properties
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  override def nodeAt(id: Long): CypherValue.Node[Long] = mapNode(
    graphStore.nodeAt(id)
  )

  override def allNodes(labels: Set[String], exactLabelMatch: Boolean): Iterable[Node[Id]] = {
    if (labels.size>1){
      throw new Exception("PandaDB doesn't support multiple label matching at the same time")
    }
    val labelIds = nodeLabelStore.ids(labels)
    val nodes: Iterator[Id] = graphStore.findNodes(labelIds.head)[Id]
    nodes.map(nodeId => mapNode(graphStore.nodeAt(nodeId))).toIterable
  }


  override def allNodes(): Iterable[Node[Id]] = {
    graphStore.allNodes().map(node => mapNode(node)).toIterable
  }

  override def allRelationships(): Iterable[CypherValue.Relationship[Id]] = {
    graphStore.allRelations().map(rel => mapRelation(rel)).toIterable
  }

  override def allRelationships(relTypes: Set[String]): Iterable[Relationship[Id]] = {
    if (relTypes.size>1){
      throw new Exception("PandaDB doesn't support multiple label matching at the same time")
    }
    val relations: Iterator[Id] = graphStore.findRelations(relLabelStore.ids(relTypes).head)[Id]
    relations.map(relId => mapRelation(graphStore.relationAt(relId))).toIterable
  }
}

class PandaPropertyGraphScanImpl(    nodeLabelStore: TokenStore,
                                     relLabelStore: TokenStore,
                                     propertyNameStore: TokenStore,
                                     nodeIdGen: FileBasedIdGen,
                                     relIdGen: FileBasedIdGen,
                                     graphStore: RocksDBGraphAPI)
      extends PropertyGraphScanImpl(nodeLabelStore, relLabelStore, nodeIdGen, relIdGen, graphStore)
      with PandaPropertyGraphScan[Long] {

  override def isPropertyWithIndex(labels: Set[String], propertyName: String): Boolean = {
    var res = false
    val labelIds = nodeLabelStore.ids(labels)
    loop.breakable({
      labelIds.foreach(label => {
        val indexId = graphStore.getNodeIndexId(label, new Array[Int](propertyNameStore.id(propertyName)))
        if (indexId != -1) {
          res = true
          loop.break()
        }
      })
    })
    res
  }

  override def allNodes(predicate: NFPredicate, labels: Set[String]): Iterable[Node[Id]] = {
    predicate match {
      case p: NFEquals => {
        val labelIds = nodeLabelStore.ids(labels)
        val propertyNameId = propertyNameStore.id(p.propName)
        var indexId = -1
        loop.breakable({
          labelIds.foreach(labelId => {
            indexId = graphStore.getNodeIndexId(labelId, Array[Int](propertyNameId))
            loop.break()
          })
        })
        if (indexId != -1) {
          var bytes: Array[Byte] = PropertyValueConverter.toBytes(p.value)
          val nodes = graphStore.findNodeIndexRecords(indexId, bytes)
          nodes.map(node => nodeAt(node)).toIterable
        }
        else {
          null
        }
      }
    }
  }


}
