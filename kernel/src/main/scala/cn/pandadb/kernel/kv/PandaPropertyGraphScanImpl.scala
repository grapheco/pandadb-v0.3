package cn.pandadb.kernel.kv

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.NameStore
import cn.pandadb.kernel.optimizer.PandaPropertyGraphScan
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import org.opencypher.lynx.PropertyGraphScanner
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

import scala.util.control._

class PropertyGraphScanImpl(nodeIdGen: FileBasedIdGen,
                            relIdGen: FileBasedIdGen,
                            nodeStore: NodeStoreSPI,
                            relationStore: RelationStoreSPI,
                            indexStore: IndexStoreAPI) extends PropertyGraphScanner[Long] {
  type Id = Long

  val loop = new Breaks

  private def mapRelation(rel: StoredRelation): Relationship[Id] = {
    new Relationship[Id] {
      override type I = this.type

      override def id: Id = rel.id

      override def startId: Id = rel.from

      override def endId: Id = rel.to

      override def relType: String = relationStore.getRelationTypeName(rel.typeId)

      override def copy(id: Id, source: Id, target: Id, relType: String, properties: CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = Map.empty[String, Any]
        if (rel.isInstanceOf[StoredRelationWithProperty]) {
          props = rel.asInstanceOf[StoredRelationWithProperty].properties.asInstanceOf[Map[String, Any]]
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  protected def mapNode(node: StoredNode): Node[Id] = {
    new Node[Id] {
      override type I = this.type

      override def id: Id = node.id

      override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeStore.getLabelName(id))

      override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = null
        if (node.isInstanceOf[StoredNodeWithProperty]) {
          props = node.asInstanceOf[StoredNodeWithProperty].properties.map{
            kv=>
              (nodeStore.getPropertyKeyName(kv._1), kv._2)
          }
        }
        else {
          val n = nodeStore.getNodeById(node.id)
          props = n.asInstanceOf[StoredNodeWithProperty].properties.map{
            kv=>
              (nodeStore.getPropertyKeyName(kv._1), kv._2)
          }
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  override def nodeAt(id: Long): CypherValue.Node[Long] = mapNode(
    nodeStore.getNodeById(id)
  )

  override def allNodes(labels: Set[String], exactLabelMatch: Boolean): Iterable[Node[Id]] = {
    if (labels.size>1){
      throw new Exception("PandaDB doesn't support multiple label matching at the same time")
    }
    val labelIds = nodeStore.getLabelIds(labels)
    val nodes = nodeStore.getNodesByLabel(labelIds.head)
    nodes.map(mapNode(_)).toIterable
  }


  override def allNodes(): Iterable[Node[Id]] = {
    nodeStore.allNodes().map(node => mapNode(node)).toIterable
  }

  override def allRelationships(): Iterable[CypherValue.Relationship[Id]] = {
    relationStore.allRelations().map(rel => mapRelation(rel)).toIterable
  }

  override def allRelationships(relTypes: Set[String]): Iterable[Relationship[Id]] = {
    if (relTypes.size>1){
      throw new Exception("PandaDB doesn't support multiple label matching at the same time")
    }
    val relations: Iterator[Id] = relationStore.getRelationIdsByRelationType(relationStore.getRelationTypeId(relTypes.head))
    relations.map(relId => mapRelation(relationStore.getRelationById(relId))).toIterable
  }
}

class PandaPropertyGraphScanImpl(nodeIdGen: FileBasedIdGen,
                                 relIdGen: FileBasedIdGen,
                                 nodeStore: NodeStoreSPI,
                                 relationStore: RelationStoreSPI,
                                 indexStore: IndexStoreAPI)
      extends PropertyGraphScanImpl(nodeIdGen, relIdGen, nodeStore, relationStore, indexStore)
      with PandaPropertyGraphScan[Long] {

  override def isPropertyWithIndex(labels: Set[String], propertyName: String): Boolean = {
    var res = false
    val labelIds = nodeStore.getLabelIds(labels)
    val propId = nodeStore.getPropertyKeyId(propertyName)
    loop.breakable({
      labelIds.foreach(label => {
        val indexId = indexStore.getIndexId(label, Array(propId))
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
        val labelIds = nodeStore.getLabelIds(labels)
        val propertyNameId = nodeStore.getPropertyKeyId(p.propName)
        var indexId = -1
        loop.breakable({
          for (labelId <- labelIds) {
            indexId = indexStore.getIndexId(labelId, Array[Int](propertyNameId))
            if (indexId != -1)
              loop.break()
          }
        })
        if (indexId != -1) {
          val nodes = indexStore.find(indexId, p.value)
          nodes.map(node => nodeAt(node)).toIterable
        } else {
          nodeStore.getNodesByLabel(labelIds.head)
            .filter(_.properties.getOrElse(propertyNameId, null)==p.value)
            .map(mapNode)
            .toIterable
//          val itr = new Iterator[Node[Id]]{
//            private def doNext(): Unit = {
//              tmpNode = null
//              loop.breakable({
//                while (nodes.hasNext) {
//                  val node = nodes.next()
//                  // fixme nodeLabelStore => propStore
//                  if (tmpNode.properties.contains(nodeLabelStore.id(p.propName)) && tmpNode.properties(nodeLabelStore.id(p.propName)) == p.value ) {
//                    loop.break()
//                  }
//                  else {
//                    tmpNode = null
//                  }
//                }
//              })
//            }
//            override def hasNext: Boolean = nodes.hasNext
//
//
//            override def next(): Node[Id] = mapNode(tmpNode)
//          }
//          itr.toIterable
        }
      }
    }
  }


}
