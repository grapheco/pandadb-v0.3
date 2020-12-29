package cn.pandadb.kernel.kv

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.{NameStore, Statistics}
import cn.pandadb.kernel.optimizer.{HasStatistics, PandaPropertyGraphScan}
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import org.opencypher.lynx.PropertyGraphScanner
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship, get}

import scala.util.control.Breaks


class PandaPropertyGraphScanImpl(nodeIdGen: FileBasedIdGen,
                                 relIdGen: FileBasedIdGen,
                                 nodeStore: NodeStoreSPI,
                                 relationStore: RelationStoreSPI,
                                 indexStore: IndexStoreAPI,
                                 statistics: Statistics)
      extends PropertyGraphScanner[Long]
      with HasStatistics
      with PandaPropertyGraphScan[Long] {

  val loop = new Breaks

  private def mapRelation(rel: StoredRelation): Relationship[Long] = {
    new Relationship[Long] {
      override type I = this.type

      override def id: Long = rel.id

      override def startId: Long = rel.from

      override def endId: Long = rel.to

      override def relType: String = relationStore.getRelationTypeName(rel.typeId).get

      override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = Map.empty[String, Any]
        rel match {
          case rel: StoredRelationWithProperty =>
            props = rel.properties.map(kv => (relationStore.getRelationTypeName(kv._1).getOrElse("unknown"), kv._2))
          case _ =>
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  protected def mapNode(node: StoredNode): Node[Long] = {
    new Node[Long] {
      override type I = this.type

      override def id: Long = node.id

      override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeStore.getLabelName(id).get)

      override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = Map.empty[String, Any]
        node match {
          case node: StoredNodeWithProperty =>
            props = node.properties.map {
              kv =>
                (nodeStore.getPropertyKeyName(kv._1).get, kv._2)
            }
          case _ =>
            val n = nodeStore.getNodeById(node.id)
            props = n.asInstanceOf[StoredNodeWithProperty].properties.map {
              kv =>
                (nodeStore.getPropertyKeyName(kv._1).get, kv._2)
            }
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  override def nodeAt(id: Long): CypherValue.Node[Long] = mapNode(
    nodeStore.getNodeById(id).orNull
  )

  override def allNodes(labels: Set[String], exactLabelMatch: Boolean): Iterable[Node[Long]] = {
    if (labels.size>1){
      throw new Exception("PandaDB doesn't support multiple label matching at the same time")
    }
    val labelIds = nodeStore.getLabelIds(labels)
    val nodes = nodeStore.getNodesByLabel(labelIds.head)
    nodes.map(mapNode(_)).toIterable
  }


  override def allNodes(): Iterable[Node[Long]] = {
    nodeStore.allNodes().map(node => mapNode(node)).toIterable
  }

  override def allRelationships(): Iterable[CypherValue.Relationship[Long]] = {
    relationStore.allRelations().map(rel => mapRelation(rel)).toIterable
  }

  override def allRelationships(relTypes: Set[String]): Iterable[Relationship[Long]] = {
    if (relTypes.size>1){
      throw new Exception("PandaDB doesn't support multiple label matching at the same time")
    }
    val relations: Iterator[Long] = relationStore.getRelationIdsByRelationType(relationStore.getRelationTypeId(relTypes.head))
    relations.map(relId => mapRelation(relationStore.getRelationById(relId).get)).toIterable
  }

  override def isPropertyWithIndex(labels: Set[String], propertyName: String): Boolean = {
    var res = false
    val labelIds = nodeStore.getLabelIds(labels)
    val propId = nodeStore.getPropertyKeyId(propertyName)
    loop.breakable({
      labelIds.foreach(label => {
        val indexId = indexStore.getIndexId(label, Array(propId))
        if (indexId.isDefined) {
          res = true
          loop.break()
        }
      })
    })
    res
  }

  override def allNodes(predicate: NFPredicate, labels: Set[String]): Iterable[Node[Long]] = {
    predicate match {
      case p: NFEquals => {
        val labelIds = nodeStore.getLabelIds(labels)
        val propertyNameId = nodeStore.getPropertyKeyId(p.propName)
        var indexId:Option[Int] = None
        loop.breakable({
          for (labelId <- labelIds) {
            indexId = indexStore.getIndexId(labelId, Array[Int](propertyNameId))
            if (indexId.isDefined)
              loop.break()
          }
        })
        if (indexId.isDefined) {
          val nodes = indexStore.find(indexId.get, p.value)
          nodes.map(node => nodeAt(node)).toIterable
        } else {
          nodeStore.getNodesByLabel(labelIds.head)
            .filter(_.properties.getOrElse(propertyNameId, null)==p.value)
            .map(mapNode)
            .toIterable
        }
      }
    }
  }

  override def isPropertyWithIndex(label: String, propertyName: String): Boolean =
    indexStore.getIndexId(nodeStore.getLabelId(label), Array(nodeStore.getPropertyKeyId(propertyName))).isDefined

  override def isPropertysWithIndex(label: String, propertyName: String*): Boolean =
    indexStore.getIndexId(nodeStore.getLabelId(label), propertyName.toArray.map(nodeStore.getPropertyKeyId)).isDefined

  override def getRelsByFilter(labels: String, direction: Int): Iterable[Relationship[Long]] =
    relationStore
      .getRelationIdsByRelationType(
        relationStore.getRelationTypeId(labels)
      )
      .map(relationStore.getRelationById(_).get)
      .map(mapRelation).toIterable


  override def getRelsByFilter(direction: Int): Iterable[Relationship[Long]] = super.getRelsByFilter(direction)

  override def getNodeById(Id: Long): Node[Long] = nodeStore.getNodeById(Id).map(mapNode).get

  override def getRelsByFilterWithProps(labels: String, direction: Int): Iterable[Relationship[Long]] = super.getRelsByFilterWithProps(labels, direction)

  override def getRelsByFilterWithProps(direction: Int): Iterable[Relationship[Long]] = super.getRelsByFilterWithProps(direction)

  override def getNodeByIdWithProps(Id: Long): Node[Long] = nodeStore.getNodeById(Id).map(mapNode).get

  override def allNodesWithProps(predicate: NFPredicate, labels: Set[String]): Iterable[Node[Long]] =
    nodeStore.allNodes().map(mapNode).toIterable

  override def getAllNodesCount(): Long = statistics.allNodesCount

  override def getNodesCountByLabel(label: String): Long =
    statistics.getNodeLabelCount(nodeStore.getLabelId(label)).getOrElse(-1)

  override def getNodesCountByLabelAndProperty(label: String, propertyName: String): Long = {
    indexStore
      .getIndexId(nodeStore.getLabelId(label), Array(nodeStore.getPropertyKeyId(propertyName)))
      .map(statistics.getIndexPropertyCount(_).getOrElse(-1L))
      .getOrElse(-1L)
  }

  override def getNodesCountByLabelAndPropertys(label: String, propertyName: String*): Long = {
    indexStore
      .getIndexId(nodeStore.getLabelId(label), propertyName.toArray.map(nodeStore.getPropertyKeyId))
      .map(statistics.getIndexPropertyCount(_).getOrElse(-1L))
      .getOrElse(-1L)
  }

  override def getAllRelsCount(): Long = statistics.allRelationCount

  override def getRelsCountByLabel(label: String): Long =
    statistics.getRelationTypeCount(relationStore.getRelationTypeId(label)).getOrElse(-1L)

  override def getRelsCountByLabelAndProperty(label: String, propertyName: String): Long = {
    indexStore
      .getIndexId(nodeStore.getLabelId(label), Array(nodeStore.getPropertyKeyId(propertyName)))
      .map(statistics.getIndexPropertyCount(_).getOrElse(-1L))
      .getOrElse(-1L)
  }

  override def getRelsCountByLabelAndPropertys(label: String, propertyName: String*): Long = {
    indexStore
      .getIndexId(nodeStore.getLabelId(label), propertyName.toArray.map(nodeStore.getPropertyKeyId))
      .map(statistics.getIndexPropertyCount(_).getOrElse(-1L))
      .getOrElse(-1L)
  }

  def refresh():Unit = {
    statistics.nodeCount = nodeStore.nodesCount
    statistics.relationCount = relationStore.relationCount

    nodeStore.allLabelIds().foreach{
      l =>
        statistics.setNodeLabelCount(l, nodeStore.getNodeIdsByLabel(l).length)
    }
    relationStore.allRelationTypeIds().foreach{
      t =>
        statistics.setRelationTypeCount(t,
          relationStore.getRelationIdsByRelationType(t).length)
    }
    indexStore.allIndexId.foreach{
      id =>
        statistics.setIndexPropertyCount(id,
          indexStore.findByPrefix(ByteUtils.intToBytes(id)).length)
    }
  }
}
