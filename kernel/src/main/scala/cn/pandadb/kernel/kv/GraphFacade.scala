package cn.pandadb.kernel.kv

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.value.ValueMappings
import cn.pandadb.kernel.store._
import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx._
import org.grapheco.lynx.cypherplus._
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName, Range, SemanticDirection}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable


class GraphFacade(nodeStore: NodeStoreSPI,
                  relationStore: RelationStoreSPI,
                  indexStore: IndexStoreAPI,
                  statistics: Statistics,
                  onClose: => Unit
                 ) extends LazyLogging with GraphService with GraphModelPlus {

  val runner = new CypherRunnerPlus(this) {
    procedures.asInstanceOf[DefaultProcedureRegistry].registerAnnotatedClass(classOf[DefaultBlobFunctions])
  }

  init()

  override def getIndexes(tx: Option[LynxTransaction]): Array[(LabelName, List[PropertyKeyName])] = {
    ???
  }

  override def cypher(query: String, parameters: Map[String, Any], tx: Option[LynxTransaction]): LynxResult = {
    runner.compile(query)
    runner.run(query, parameters, tx)
  }

  override def close(): Unit = {
    statistics.flush()
    statistics.close()
    nodeStore.close()
    relationStore.close()
    indexStore.close()
  }

  private def nodeLabelNameMap(name: String): Option[Int] = nodeStore.getLabelId(name)

  private def nodePropNameMap(name: String): Option[Int] = nodeStore.getPropertyKeyId(name)

  private def relTypeNameMap(name: String): Option[Int] = relationStore.getRelationTypeId(name)

  private def relPropNameMap(name: String): Option[Int] = relationStore.getPropertyKeyId(name)

  override def addNode(nodeProps: Map[String, Any], labels: String*): Id = {
    addNode(None, labels, nodeProps)
  }

  private def addNode(id: Option[Long], labels: Seq[String], nodeProps: Map[String, Any]): Id = {
    val nodeId = id.getOrElse(nodeStore.newNodeId())
    val labelIds = labels.map(nodeStore.addLabel).toArray
    val props = nodeProps.map(v => (nodeStore.addPropertyKey(v._1), v._2))
    nodeStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, props))
    statistics.increaseNodeCount(1) // TODO batch
    labelIds.foreach(statistics.increaseNodeLabelCount(_, 1))
    // index
    labelIds.map(indexStore.getIndexIdByLabel).foreach(
      _.foreach {
        propsIndex =>
          if (propsIndex.props.length <= 1) {
            indexStore.insertIndexRecord(propsIndex.indexId, props.getOrElse(propsIndex.props.head, null), nodeId)
          } else {
            // TODO combined index
          }
          statistics.increaseIndexPropertyCount(propsIndex.indexId, 1)
      })
    nodeId
  }

  override def addRelation(label: String, from: Id, to: Id, relProps: Map[String, Any]): Id = {
    addRelation(None, label, from, to, relProps)
  }

  def addRelationFromPhysical(id: Long, label: String, from: Id, to: Id, relProps: Map[String, Any]): Id = {
    addRelation(Some(id), label: String, from: Id, to: Id, relProps: Map[String, Any])
  }

  private def addRelation(id: Option[Long], label: String, from: Long, to: Long, relProps: Map[String, Any]): Id = {
    val rid = id.getOrElse(relationStore.newRelationId())
    val labelId = relationStore.addRelationType(label)
    val props = relProps.map(v => (relationStore.addPropertyKey(v._1), v._2))
    val rel = new StoredRelationWithProperty(rid, from, to, labelId, props)
    //TODO: transaction safe
    relationStore.addRelation(rel)
    statistics.increaseRelationCount(1) // TODO batch , index statistic
    statistics.increaseRelationTypeCount(labelId, 1)
    rid
  }

  override def deleteNode(id: Id): Unit = {
    nodeStore.getNodeById(id).foreach {
      node =>
        nodeStore.deleteNode(node.id)
        statistics.decreaseNodes(1)
        node.labelIds.foreach(statistics.decreaseNodeLabelCount(_, 1))
        node.labelIds.map(indexStore.getIndexIdByLabel).foreach(
          _.foreach {
            propsIndex =>
              if (propsIndex.props.length <= 1) {
                indexStore.deleteIndexRecord(propsIndex.indexId, node.properties.getOrElse(propsIndex.props.head, null), node.id)
              } else {
                // TODO combined index
              }
              statistics.decreaseIndexPropertyCount(propsIndex.indexId, 1)
          })
    }
  }

  override def filterNodesWithRelations(nodesIDs: Seq[LynxId], tx: Option[LynxTransaction]): Seq[LynxId] = { //TODO opt perf.
    nodesIDs.filter(nid => {
      val nidL = nid.asInstanceOf[NodeId].value
      relationStore.findOutRelations(nidL).nonEmpty || relationStore.findInRelations(nidL).nonEmpty
    })
  }

  override def deleteRelationsOfNodes(nodesIDs: Seq[LynxId], tx: Option[LynxTransaction]): Unit = { //TODO opt perf.
    nodesIDs.foreach(nid => {
      val nidL = nid.asInstanceOf[NodeId].value
      relationStore.findOutRelations(nidL).foreach(rel => relationStore.deleteRelation(rel.id))
      relationStore.findInRelations(nidL).foreach(rel => relationStore.deleteRelation(rel.id))
    })
  }

  override def deleteFreeNodes(nodesIDs: Seq[LynxId], tx: Option[LynxTransaction]): Unit = {
    nodeStore.deleteNodes(nodesIDs.map(_.asInstanceOf[NodeId].value).toIterator)
  }

  override def deleteRelation(id: Id): Unit = {
    relationStore.getRelationById(id).foreach {
      rel =>
        relationStore.deleteRelation(rel.id)
        statistics.decreaseRelations(1)
        statistics.setRelationTypeCount(rel.typeId, 1)
    }
  }

  def allNodes(): Iterable[StoredNodeWithProperty] = {
    nodeStore.allNodes().toIterable
  }

  def allRelations(): Iterable[StoredRelation] = {
    relationStore.allRelations().toIterable
  }

  def writeNodeIndexRecord(indexId: Int, nodeId: Long, propertyValue: Any): Unit = {
    indexStore.insertIndexRecord(indexId, propertyValue, nodeId)
  }

  override def nodeSetProperty(id: Id, key: String, value: Any): Unit =
    nodeStore.nodeSetProperty(id, nodeStore.addPropertyKey(key), value)


  override def nodeRemoveProperty(id: Id, key: String): Unit =
    nodePropNameMap(key).foreach(nodeStore.nodeRemoveProperty(id, _))

  override def nodeAddLabel(id: Id, label: String): Unit =
    nodeStore.nodeAddLabel(id, nodeStore.addLabel(label))

  override def nodeRemoveLabel(id: Id, label: String): Unit =
    nodeLabelNameMap(label).foreach(nodeStore.nodeRemoveLabel(id, _))

  override def relationSetProperty(id: Id, key: String, value: Any): Unit =
    relationStore.relationSetProperty(id, relationStore.addPropertyKey(key), value)

  override def relationRemoveProperty(id: Id, key: String): Unit =
    relPropNameMap(key).foreach(relationStore.relationRemoveProperty(id, _))

  override def relationAddLabel(id: Id, label: String): Unit = ???

  override def relationRemoveLabel(id: Id, label: String): Unit = ???

  override def createIndexOnNode(label: String, props: Set[String]): Unit = {
    val labelId = nodeStore.addLabel(label)
    val propsId = props.map(nodeStore.addPropertyKey).toArray.sorted
    val indexId = indexStore.createIndex(labelId, propsId)
    if (propsId.length == 1) {
      indexStore.insertIndexRecordBatch(
        indexId,
        nodeStore.getNodesByLabel(labelId).map {
          node =>
            (node.properties.getOrElse(propsId(0), null), node.id)
        }
      )
      statistics.setIndexPropertyCount(indexId, nodeStore.getNodesByLabel(labelId).length)
    } else {
      // TODO combined index
    }
  }

  override def createIndexOnRelation(typeName: String, props: Set[String]): Unit = {
    val typeId = relationStore.addRelationType(typeName)
    val propsId = props.map(relationStore.addPropertyKey).toArray.sorted
    val indexId = indexStore.createIndex(typeId, propsId)
    if (propsId.length == 1) {
      indexStore.insertIndexRecordBatch(
        indexId,
        relationStore.getRelationIdsByRelationType(typeId)
          .map(relationStore.getRelationById(_).get)
          .map {
            rel =>
              (rel.properties.getOrElse(propsId(0), null), rel.id)
          }
      )
      statistics.setIndexPropertyCount(indexId, relationStore.getRelationIdsByRelationType(typeId).length)
    } else {
      // TODO combined index
    }
  }

  override def createIndex(labelName: LabelName, properties: List[PropertyKeyName], tx: Option[LynxTransaction]): Unit = {
    val l = labelName.name
    val ps = properties.map(p => p.name).toSet
    createIndexOnNode(l, ps)
    createIndexOnRelation(l, ps)
  }

  def refresh(): Unit = {
    statistics.nodeCount = nodeStore.nodesCount
    logger.info(s"node count: ${statistics.nodeCount}")
    statistics.relationCount = relationStore.relationCount
    logger.info(s"relation count: ${statistics.relationCount}")
    nodeStore.allLabelIds().foreach {
      l =>
        statistics.setNodeLabelCount(l, nodeStore.getNodeIdsByLabel(l).length)
        logger.info(s"label ${l} count: ${statistics.getNodeLabelCount(l)}")
    }
    relationStore.allRelationTypeIds().foreach {
      t =>
        statistics.setRelationTypeCount(t,
          relationStore.getRelationIdsByRelationType(t).length)
        logger.info(s"type ${t} count: ${statistics.getRelationTypeCount(t)}")
    }
    indexStore.allIndexId.foreach {
      meta =>
        statistics.setIndexPropertyCount(meta.indexId,
          indexStore.findByPrefix(ByteUtils.intToBytes(meta.indexId)).length)
        logger.info(s"index ${meta.indexId} count: ${statistics.getIndexPropertyCount(meta.indexId)}")
    }
    statistics.flush()
  }

  //FIXME: expensive time cost
  private def init(): Unit = {
    statistics.init()
  }

  def snapshot(): Unit = {
    //TODO: transaction safe
  }

  protected def mapNode(node: StoredNode): PandaNode = {
    PandaNode(node.id,
      node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq,
      node.properties.map(kv => (nodeStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq: _*)
  }

  protected def mapRelation(rel: StoredRelation): PandaRelationship = {
    PandaRelationship(rel.id,
      rel.from, rel.to,
      relationStore.getRelationTypeName(rel.typeId),
      rel.properties.map(kv => (relationStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq: _*)
  }

  //TODO need props?
  def rels(includeStartNodes: Boolean, includeEndNodes: Boolean): Iterator[(PandaRelationship, Option[PandaNode], Option[PandaNode])] = {
    relationStore.allRelations(withProperty = true).map(mapRelation).map { rel =>
      (rel,
        if (includeStartNodes) nodeAt(rel.startNodeId) else None,
        if (includeEndNodes) nodeAt(rel.endNodeId) else None)
    }
  }

  override def nodes(tx: Option[LynxTransaction]): Iterator[PandaNode] = nodeStore.allNodes().map(mapNode)

  def nodeAt(id: LynxId): Option[PandaNode] = nodeAt(id.value.asInstanceOf[Long])

  def nodeAt(id: Long): Option[PandaNode] = nodeStore.getNodeById(id).map(mapNode)

  def nodeAt(id: Long, label: String): Option[PandaNode] =
    nodeLabelNameMap(label).map(nodeStore.getNodeById(id, _).orNull).map(mapNode)

  def nodeHasLabel(id: Long, label: String): Boolean = nodeLabelNameMap(label).exists(nodeStore.hasLabel(id, _))

  def relationAt(id: Long): Option[PandaRelationship] = relationStore.getRelationById(id).map(mapRelation)

  def relationAt(id: LynxId): Option[PandaRelationship] = relationAt(id.value.asInstanceOf[Long])


  def rels(types: Seq[String],
           labels1: Seq[String],
           labels2: Seq[String],
           includeStartNodes: Boolean,
           includeEndNodes: Boolean): Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] = {
    val includeProperty = false
    rels(types, labels1, labels2, includeProperty, includeStartNodes, includeEndNodes)
  }

  def rels(types: Seq[String],
           labels1: Seq[String],
           labels2: Seq[String],
           includeProperty: Boolean,
           includeStartNodes: Boolean,
           includeEndNodes: Boolean): Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] = {

    if (labels1.map(nodeLabelNameMap).contains(None) ||
      labels2.map(nodeLabelNameMap).contains(None) ||
      types.map(relTypeNameMap).contains(None)) {
      Iterator.empty
    } else {
      val label1Id = labels1.map(nodeLabelNameMap).map(_.get).sorted
      val label2Id = labels2.map(nodeLabelNameMap).map(_.get).sorted
      val typeIds = types.map(relTypeNameMap).map(_.get).sorted
      (label1Id.nonEmpty, typeIds.nonEmpty, label2Id.nonEmpty) match {
        case (_, true, true) => // [nodesWithLabel1]-[relWithTypes]-[nodesWithLabel2]
          // TODO if types is small
          typeIds.map(
            typeId =>
              getNodesByLabels(labels1, exact = false).flatMap {
                startNode =>
                  relationStore
                    .findOutRelations(startNode.id, Some(typeId))
                    .map(rel => (rel, nodeStore.getNodeLabelsById(rel.to)))
                    .withFilter(_._2.toSeq.containsSlice(label2Id))
                    .map(relEnd => (
                      if (includeProperty) relationAt(relEnd._1.id).orNull else mapRelation(relEnd._1),
                      if (includeStartNodes) Some(mapNode(startNode)) else None,
                      if (includeEndNodes) nodeStore.getNodeById(relEnd._1.to, relEnd._2.headOption).map(mapNode) else None
                    ))
              }
          ).reduce(_ ++ _)
        case (_, false, true) => // [nodesWithLabel1]-[rels]-[nodesWithLabel2]
          getNodesByLabels(labels1, exact = false).flatMap {
            startNode =>
              relationStore
                .findOutRelations(startNode.id)
                .map(rel => (rel, nodeStore.getNodeLabelsById(rel.to)))
                .withFilter(_._2.toSeq.containsSlice(label2Id))
                .map(relEnd => (
                  if (includeProperty) relationAt(relEnd._1.id).orNull else mapRelation(relEnd._1),
                  if (includeStartNodes) Some(mapNode(startNode)) else None,
                  if (includeEndNodes) nodeStore.getNodeById(relEnd._1.to, relEnd._2.headOption).map(mapNode) else None
                ))
          }
        case (_, true, false) => // [nodesWithLabel1]-[relsWithTypes]-[nodes]
          typeIds.map(
            typeId =>
              getNodesByLabels(labels1, exact = false).flatMap {
                startNode =>
                  relationStore
                    .findOutRelations(startNode.id, Some(typeId))
                    .map(rel => (
                      if (includeProperty) relationAt(rel.id).orNull else mapRelation(rel),
                      if (includeStartNodes) Some(mapNode(startNode)) else None,
                      if (includeEndNodes) nodeAt(rel.to) else None
                    ))
              }
          ).reduce(_ ++ _)
        case (true, false, false) => // [nodesWithLabel1]-[allRel]-[allNodes]
          getNodesByLabels(labels1, exact = false).flatMap {
            startNode =>
              relationStore
                .findOutRelations(startNode.id)
                .map(rel => (
                  if (includeProperty) relationAt(rel.id).orNull else mapRelation(rel),
                  if (includeStartNodes) Some(mapNode(startNode)) else None,
                  if (includeEndNodes) nodeAt(rel.to) else None
                ))
          }
        case (false, false, false) => // [allRelations]
          relationStore
            .allRelations(withProperty = includeProperty)
            .map(rel => (
              mapRelation(rel),
              if (includeStartNodes) nodeAt(rel.from) else None,
              if (includeEndNodes) nodeAt(rel.to) else None
            ))
      }
    }
  }

  def nodes(labels: Seq[String], exact: Boolean): Iterator[PandaNode] = getNodesByLabels(labels, exact).map(mapNode)

  def getNodesByLabels(labels: Seq[String], exact: Boolean): Iterator[StoredNode] = {
    if (labels.isEmpty) {
      nodeStore.allNodes()
    } else if (labels.size == 1) {
      labels.map(nodeLabelNameMap)
        .head
        .map(nodeStore.getNodesByLabel(_).filterNot(exact && _.labelIds.length > 1))
        .getOrElse(Iterator.empty)
    } else {
      //TODO statistics choose one min count
      val labelIds = labels.map(nodeLabelNameMap).sorted
      val label = nodeLabelNameMap(labels.head) // head is None, if has None
      label.map {
        nodeStore.getNodesByLabel(_).filter {
          if (exact)
            _.labelIds.sorted.toSeq == labelIds
          else
            _.labelIds.sorted.containsSlice(labelIds)
        }
      }.getOrElse(Iterator.empty)
    }
  }

  def filterNodes(p: PandaNode, properties: Map[String, LynxValue]): Boolean =
    properties.forall(x => p.properties.exists(x.equals))

  def hasIndex(labels: Set[String], propertyNames: Set[String]): Option[(Int, String, Set[String], Long)] = {
    val propertyIds = propertyNames.flatMap(nodeStore.getPropertyKeyId).toArray.sorted
    val labelIds = labels.flatMap(nodeLabelNameMap)
    val combinations = propertyIds.toSet.subsets().drop(1)
    val indexes = labelIds.flatMap(
      label => combinations.flatMap(
        props => indexStore.getIndexId(label, props.toArray).map((_, label, props, 0L))
      )
    )
    val res = indexes.flatMap(p => statistics.getIndexPropertyCount(p._1).map((p._1, p._2, p._3, _)))
    res.headOption
      .map(h => res.minBy(_._4))
      .orElse(indexes.headOption)
      .map(res => (res._1, nodeStore.getLabelName(res._2).get, res._3.map(nodeStore.getPropertyKeyName(_).get), res._4))
  }

  def findNodeByIndex(indexId: Int, value: Any): Iterator[PandaNode] =
    indexStore.find(indexId, value).flatMap(nodeStore.getNodeById(_)).map(mapNode)

  override def paths(startNodeFilter: NodeFilter,
                     relationshipFilter: RelationshipFilter,
                     endNodeFilter: NodeFilter,
                     direction: SemanticDirection, tx: Option[LynxTransaction]): Iterator[PathTriple] =
    nodes(startNodeFilter, None).flatMap(node => paths(node.id, relationshipFilter, endNodeFilter, direction))


  def paths(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] =
    nodeAt(nodeId).map(
      n =>
        direction match {
          case SemanticDirection.INCOMING => relationStore.findInRelations(n.longId).map(r => (n, r, r.from))
          case SemanticDirection.OUTGOING => relationStore.findOutRelations(n.longId).map(r => (n, r, r.to))
          case SemanticDirection.BOTH => relationStore.findInRelations(n.longId).map(r => (n, r, r.from)) ++
            relationStore.findOutRelations(n.longId).map(r => (n, r, r.to))
        }
    )
      .getOrElse(Iterator.empty)
      .map(p => PathTriple(p._1, relationAt(p._2.id).orNull, nodeAt(p._3).orNull))

  //  override def paths(nodeId: LynxId,
  //                     relationshipFilter: RelationshipFilter,
  //                     endNodeFilter: NodeFilter,
  //                     direction: SemanticDirection): Iterator[PathTriple] =
  //    paths(nodeId, direction) // TODO 这里应把关系过滤作为查询条件而不是过滤条件
  //      .filter(trip => relationshipFilter.matches(trip.storedRelation) && endNodeFilter.matches(trip.endNode))

  def paths(nodeId: LynxId,
            relationshipFilter: RelationshipFilter,
            endNodeFilter: NodeFilter,
            direction: SemanticDirection): Iterator[PathTriple] = {
    nodeAt(nodeId).map(
      node => {
        if (relationshipFilter.types.nonEmpty) {
          relationshipFilter.types.map(relTypeNameMap).map(
            _.map(
              relType =>
                direction match {
                  case SemanticDirection.INCOMING => relationStore.findInRelations(node.longId, Some(relType)).map(r => (node, r, r.from))
                  case SemanticDirection.OUTGOING => relationStore.findOutRelations(node.longId, Some(relType)).map(r => (node, r, r.to))
                  case SemanticDirection.BOTH => relationStore.findInRelations(node.longId, Some(relType)).map(r => (node, r, r.from)) ++
                    relationStore.findOutRelations(node.longId, Some(relType)).map(r => (node, r, r.to))
                }
            ).getOrElse(Iterator.empty)
          )
        } else {
          Seq(direction match {
            case SemanticDirection.INCOMING => relationStore.findInRelations(node.longId).map(r => (node, r, r.from))
            case SemanticDirection.OUTGOING => relationStore.findOutRelations(node.longId).map(r => (node, r, r.to))
            case SemanticDirection.BOTH => relationStore.findInRelations(node.longId).map(r => (node, r, r.from)) ++
              relationStore.findOutRelations(node.longId).map(r => (node, r, r.to))
          })
        }
      }
    )
      .getOrElse(Iterator.empty)
      .reduce(_ ++ _)
      .map(p => PathTriple(p._1, relationAt(p._2.id).orNull, nodeAt(p._3).orNull))
      .filter(trip => endNodeFilter.matches(trip.endNode))
  }

  def middlePaths(nodeId: LynxId,
                  relationshipFilter: RelationshipFilter,
                  direction: SemanticDirection): Iterator[PathTriple] = {
    nodeAt(nodeId).map(
      node => {
        if (relationshipFilter.types.nonEmpty) {
          relationshipFilter.types.map(relTypeNameMap).map(
            _.map(
              relType =>
                direction match {
                  case SemanticDirection.INCOMING => relationStore.findInRelations(node.longId, Some(relType)).map(r => (node, r, r.from))
                  case SemanticDirection.OUTGOING => relationStore.findOutRelations(node.longId, Some(relType)).map(r => (node, r, r.to))
                  case SemanticDirection.BOTH => relationStore.findInRelations(node.longId, Some(relType)).map(r => (node, r, r.from)) ++
                    relationStore.findOutRelations(node.longId, Some(relType)).map(r => (node, r, r.to))
                }
            ).getOrElse(Iterator.empty)
          )
        } else {
          Seq(direction match {
            case SemanticDirection.INCOMING => relationStore.findInRelations(node.longId).map(r => (node, r, r.from))
            case SemanticDirection.OUTGOING => relationStore.findOutRelations(node.longId).map(r => (node, r, r.to))
            case SemanticDirection.BOTH => relationStore.findInRelations(node.longId).map(r => (node, r, r.from)) ++
              relationStore.findOutRelations(node.longId).map(r => (node, r, r.to))
          })
        }
      }
    )
      .getOrElse(Iterator.empty)
      .reduce(_ ++ _)
      .map(p => PathTriple(p._1, relationAt(p._2.id).orNull, nodeAt(p._3).orNull))
  }

  override def expand(nodeId: LynxId, direction: SemanticDirection, tx: Option[LynxTransaction]): Iterator[PathTriple] = {
    val rels = direction match {
      case SemanticDirection.INCOMING => relationStore.findInRelations(nodeId.value.asInstanceOf[Long]).map(r => {
        val toNode = mapNode(nodeStore.getNodeById(r.to).get)
        val rel = mapRelation(r)
        val fromNode = mapNode(nodeStore.getNodeById(r.from).get)
        PathTriple(toNode, rel, fromNode)
      })
      case SemanticDirection.OUTGOING => relationStore.findOutRelations(nodeId.value.asInstanceOf[Long]).map(r => {
        val toNode = mapNode(nodeStore.getNodeById(r.to).get)
        val rel = mapRelation(r)
        val fromNode = mapNode(nodeStore.getNodeById(r.from).get)
        PathTriple(fromNode, rel, toNode)
      })
      case SemanticDirection.BOTH => relationStore.findInRelations(nodeId.value.asInstanceOf[Long]).map(r => {
        val toNode = mapNode(nodeStore.getNodeById(r.to).get)
        val rel = mapRelation(r)
        val fromNode = mapNode(nodeStore.getNodeById(r.from).get)
        PathTriple(toNode, rel, fromNode)
      }) ++
        relationStore.findOutRelations(nodeId.value.asInstanceOf[Long]).map(r => {
          val toNode = mapNode(nodeStore.getNodeById(r.to).get)
          val rel = mapRelation(r)
          val fromNode = mapNode(nodeStore.getNodeById(r.from).get)
          PathTriple(fromNode, rel, toNode)
        })
    }
    rels
  }

  override def expand(nodeId: LynxId, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection, tx: Option[LynxTransaction]): Iterator[PathTriple] = {
    // has properties?
    endNodeFilter.properties.toSeq match {
      case Seq() => expand(nodeId, direction, relationshipFilter).filter(
        item => {
          val PathTriple(_, rel, endNode, _) = item
          relationshipFilter.matches(rel) && endNodeFilter.labels.forall(endNode.labels.contains(_))
        }
      )
      case _ => expand(nodeId, direction, relationshipFilter).filter(
        item => {
          val PathTriple(_, rel, endNode, _) = item
          relationshipFilter.matches(rel) && endNodeFilter.matches(endNode)
        }
      )
    }
  }

  // has relationship types?
  def expand(nodeId: LynxId, direction: SemanticDirection, relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    val typeId = {
      relationshipFilter.types match {
        case Seq() => None
        // TODO: multiple relation types: use statistic to get least num of type
        case _ => relationStore.getRelationTypeId(relationshipFilter.types.head)
      }
    }
    if (typeId.isEmpty && relationshipFilter.types.nonEmpty) {
      Iterator.empty
    } else {
      direction match {
        case SemanticDirection.INCOMING => relationStore.findInRelations(nodeId.value.asInstanceOf[Long], typeId).map(r => {
          val toNode = LazyPandaNode(r.to, nodeStore)
          val rel = mapRelation(r)
          val fromNode = LazyPandaNode(r.from, nodeStore)
          PathTriple(toNode, rel, fromNode)
        })

        case SemanticDirection.OUTGOING => relationStore.findOutRelations(nodeId.value.asInstanceOf[Long], typeId).map(r => {
          val toNode = LazyPandaNode(r.to, nodeStore)
          val rel = mapRelation(r)
          val fromNode = LazyPandaNode(r.from, nodeStore)
          PathTriple(fromNode, rel, toNode)
        })
        case SemanticDirection.BOTH => relationStore.findInRelations(nodeId.value.asInstanceOf[Long], typeId).map(r => {
          val toNode = LazyPandaNode(r.to, nodeStore)
          val rel = mapRelation(r)
          val fromNode = LazyPandaNode(r.from, nodeStore)
          PathTriple(toNode, rel, fromNode)
        }) ++
          relationStore.findOutRelations(nodeId.value.asInstanceOf[Long], typeId).map(r => {
            val toNode = LazyPandaNode(r.to, nodeStore)
            val rel = mapRelation(r)
            val fromNode = LazyPandaNode(r.from, nodeStore)
            PathTriple(fromNode, rel, toNode)
          })
      }
    }
  }

  override def nodes(nodeFilter: NodeFilter, tx: Option[LynxTransaction]): Iterator[PandaNode] = {
    (nodeFilter.labels.nonEmpty, nodeFilter.properties.nonEmpty) match {
      case (false, false) => allNodes().iterator.map(mapNode)
      case (true, false) => nodes(nodeFilter.labels, exact = false)
      case (false, true) => allNodes().iterator.map(mapNode).filter(filterNodes(_, nodeFilter.properties))
      case (true, true) =>
        hasIndex(nodeFilter.labels.toSet, nodeFilter.properties.keys.toSet)
          .map {
            indexInfo => // fixme ------------------------------------only get one prop ↓
              logger.info("seek index: " + indexInfo)
              findNodeByIndex(indexInfo._1, nodeFilter.properties.getOrElse(indexInfo._3.head, null).value)
                .filter(x => filterNodes(x, nodeFilter.properties))
          }
          .getOrElse {
            logger.info("scan label")
            getNodesByLabels(nodeFilter.labels, exact = false)
              .map(mapNode)
              .filter(filterNodes(_, nodeFilter.properties))
          }
    }
  }

  override def relationships(tx: Option[LynxTransaction]): Iterator[PathTriple] =
    allRelations().toIterator.map(rel => PathTriple(nodeAt(rel.from).get, mapRelation(rel), nodeAt(rel.to).get))

  override def createElements[T](nodesInput: Seq[(String, NodeInput)], relsInput: Seq[(String, RelationshipInput)], onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T, tx: Option[LynxTransaction]): T = {
    val nodesMap: Seq[(String, PandaNode)] = nodesInput.map(x => {
      val (varname, input) = x
      val id = nodeStore.newNodeId()
      varname -> PandaNode(id, input.labels, input.props: _*)
    })

    def nodeId(ref: NodeInputRef): Long = {
      ref match {
        case StoredNodeInputRef(id) => id.value.asInstanceOf[Long]
        //case ContextualNodeInputRef(node) => nodesMap(node)._2.longId
        case ContextualNodeInputRef(varname) => nodesMap.find(_._1 == varname).get._2.longId
      }
    }

    val relsMap: Seq[(String, PandaRelationship)] = relsInput.map(x => {
      val (varname, input) = x
      varname -> PandaRelationship(relationStore.newRelationId(), nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption, input.props: _*)
    }
    )

    nodesMap.foreach {
      node => addNode(Some(node._2.longId), node._2.labels, node._2.props.toMap.mapValues(ValueMappings.lynxValueMappingToScala))
    }

    relsMap.foreach {
      rel => {
        //        addRelation(rel._2.relationType.get, rel._2.startId, rel._2.endId, rel._2.properties.mapValues(_.value))
        addRelationFromPhysical(rel._2._id, rel._2.relationType.get, rel._2.startId, rel._2.endId, rel._2.properties.mapValues(ValueMappings.lynxValueMappingToScala))
      }
    }

    onCreated(nodesMap, relsMap)
  }

  override def setNodeProperty(nodeId: LynxId, data: Array[(String, Any)], cleanExistProperties: Boolean = false, tx: Option[LynxTransaction]): Option[LynxNode] = {
    val node = nodeAt(nodeId)
    if (node.isDefined) {
      if (cleanExistProperties) {
        node.get.properties.keys.foreach(key => nodeRemoveProperty(nodeId.value.asInstanceOf[Long], key))
      }
      data.foreach(kv => nodeSetProperty(nodeId.value.asInstanceOf[Long], kv._1, kv._2))
      nodeAt(nodeId)
    }
    else None
  }

  override def addNodeLabels(nodeId: LynxId, labels: Array[String], tx: Option[LynxTransaction]): Option[LynxNode] = {
    labels.foreach(label => nodeAddLabel(nodeId.value.asInstanceOf[Long], label))
    nodeAt(nodeId)
  }

  override def setRelationshipProperty(triple: Seq[LynxValue], data: Array[(String, Any)], tx: Option[LynxTransaction]): Option[Seq[LynxValue]] = {
    val relId = triple(1).asInstanceOf[LynxRelationship].id.value.asInstanceOf[Long]
    data.foreach(kv => relationSetProperty(relId, kv._1, kv._2))
    val relationship = relationAt(relId)
    if (relationship.isDefined) Option(Seq(triple.head, relationship.get, triple(2)))
    else None
  }

  // can not do this
  override def setRelationshipTypes(triple: Seq[LynxValue], labels: Array[String], tx: Option[LynxTransaction]): Option[Seq[LynxValue]] = ???

  override def removeNodeProperty(nodeId: LynxId, data: Array[String], tx: Option[LynxTransaction]): Option[LynxNode] = {
    data.foreach(key => nodeRemoveProperty(nodeId.value.asInstanceOf[Long], key))
    nodeAt(nodeId)
  }

  override def removeNodeLabels(nodeId: LynxId, labels: Array[String], tx: Option[LynxTransaction]): Option[LynxNode] = {
    labels.foreach(label => nodeRemoveLabel(nodeId.value.asInstanceOf[Long], label))
    nodeAt(nodeId)
  }

  override def removeRelationshipProperty(triple: Seq[LynxValue], data: Array[String], tx: Option[LynxTransaction]): Option[Seq[LynxValue]] = {
    val relId = triple(1).asInstanceOf[LynxRelationship].id.value.asInstanceOf[Long]
    data.foreach(key => relationRemoveProperty(relId, key))
    val relationship = relationAt(relId)
    if (relationship.isDefined) Option(Seq(triple.head, relationship.get, triple(2)))
    else None
  }

  // can not do this
  override def removeRelationshipType(triple: Seq[LynxValue], labels: Array[String], tx: Option[LynxTransaction]): Option[Seq[LynxValue]] = ???

  override def getSubProperty(value: LynxValue, propertyKey: String): LynxValue = {
    //    propertyKey match {
    //      case "faceFeature" => {
    //        val client = new FaceFeatureClient("10.0.90.173:8081")
    //        val feature = client.getFaceFeatures(value.value.asInstanceOf[Blob].toBytes())
    //        val result: LynxList = LynxList(feature.map(list => LynxList(list.map(item => LynxDouble(item)))))
    //        result
    //      }
    //    }
    ???
  }

  override def getSemanticComparator(algoName: Option[String]): SemanticComparator = {
    new SemanticComparator {
      override def compare(a: LynxValue, b: LynxValue): Option[Double] = Some(0.1)
    }
  }

  override def getInternalBlob(bid: String): Blob = {
    Blob.EMPTY
  }

  override def estimateNodeLabel(labelName: String): Id = 1

  def estimateNodeProperty(labelName: String, propertyName: String, value: AnyRef): Id = 1

  override def estimateRelationship(relType: String): Id = 1

  override def pathsWithLength(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection, length: Option[Option[expressions.Range]], tx: Option[LynxTransaction]): Iterator[Seq[PathTriple]] = {
    def getDegreeRelationship(lower: Int, upper: Int): Iterator[Seq[PathTriple]] = {
      val searchedPaths = ArrayBuffer[Iterator[Seq[PathTriple]]]()
      val middlePathIterator = mutable.Map[Int, Iterator[Seq[PathTriple]]]()

      for (degree <- lower to upper) {
        degree match {
          case 0 => {
            val res = nodes(startNodeFilter, tx).map(node => Seq(PathTriple(node, null, node)))
            searchedPaths += res
          }
          case 1 => {
            val res = nodes(startNodeFilter, tx).flatMap(f => paths(f.id, relationshipFilter, endNodeFilter, direction).map(Seq(_)))
            searchedPaths += res
          }
          case n => {
            var left = Iterator[Seq[PathTriple]]()
            if (n == 2) {
              left = nodes(startNodeFilter, tx).flatMap(f => middlePaths(f.id, relationshipFilter, direction)).map(Seq(_))
            }
            else {
              val dupIter = middlePathIterator(n - 1).duplicate
              middlePathIterator(n - 1) = dupIter._1
              left = dupIter._2
            }
            left = left.flatMap(leftTriple => {
              val rels = middlePaths(leftTriple.last.endNode.id, relationshipFilter, direction)
                .filter(f => !leftTriple.map(l => l.storedRelation).contains(f.storedRelation))

              rels.map(f => leftTriple ++ Seq(f))
            })
            val dup2 = left.duplicate
            middlePathIterator += n -> dup2._1
            searchedPaths += dup2._2.filter(f => endNodeFilter.matches(f.last.endNode))
          }
        }
      }
      searchedPaths.foldLeft(Iterator[Seq[PathTriple]]())((res, iterator) => res ++ iterator)
    }

    length match {
      case Some(None) => getDegreeRelationship(1, Int.MaxValue)
      case Some(Some(range)) => {
        range match {
          case Range(None, None) => getDegreeRelationship(1, Int.MaxValue)
          case Range(lower, None) => getDegreeRelationship(lower.get.value.toInt, Int.MaxValue)
          case Range(None, upper) => getDegreeRelationship(1, upper.get.value.toInt)
          case Range(lower, upper) => getDegreeRelationship(lower.get.value.toInt, upper.get.value.toInt)
        }
      }
    }
  }

  override def copyNode(srcNode: LynxNode, maskNode: LynxNode, tx: Option[LynxTransaction]): Seq[LynxValue] = ???

  override def mergeNode(nodeFilter: NodeFilter, forceToCreate: Boolean, tx: Option[LynxTransaction]): LynxNode = ???

  override def mergeRelationship(relationshipFilter: RelationshipFilter, leftNode: LynxNode, rightNode: LynxNode, direction: SemanticDirection, forceToCreate: Boolean, tx: Option[LynxTransaction]): PathTriple = ???

  override def deleteRelation(id: LynxId, tx: Option[LynxTransaction]): Unit = ???

  override def deleteRelations(ids: Iterator[LynxId], tx: Option[LynxTransaction]): Unit = ???
}
