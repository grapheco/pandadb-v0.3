package org.grapheco.pandadb.kernel.distribute

import org.grapheco.pandadb.kernel.kv.value.ValueMappings
import org.grapheco.pandadb.kernel.store.{NodeId, PandaNode, PandaRelationship, RelationId}
import org.grapheco.lynx.{ContextualNodeInputRef, GraphModel, Index, IndexManager, LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType, LynxValue, NodeFilter, NodeInput, NodeInputRef, PathTriple, RelationshipFilter, RelationshipInput, Statistics, StoredNodeInputRef, WriteTask}
import org.grapheco.pandadb.kernel.util.PandaDBException.PandaDBException
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.mutable


/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-19 13:36
 */
class GraphParseModel(db: DistributedGraphService) extends GraphModel {
  implicit def lynxId2NodeId(lynxId: LynxId): NodeId = NodeId(lynxId.value.asInstanceOf[Long])

  implicit def lynxId2RelationId(lynxId: LynxId): RelationId = RelationId(lynxId.value.asInstanceOf[Long])


  override def statistics: Statistics = new Statistics {
    override def numNode: Long = db.getStatistics.nodeCount

    override def numNodeByLabel(labelName: LynxNodeLabel): Long =
      db.getStatistics.getNodeLabelCount(db.getNodeLabelId(labelName.value).getOrElse(-1)).getOrElse(0)

    override def numNodeByProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long =
      db.getStatistics.getIndexPropertyCount(db.getPropertyId(propertyName.value).getOrElse(-1)).getOrElse(0)

    override def numRelationship: Long = db.getStatistics.relationCount

    override def numRelationshipByType(typeName: LynxRelationshipType): Long =
      db.getStatistics.getRelationTypeCount(db.getRelationTypeId(typeName.value).getOrElse(-1)).getOrElse(0)
  }


  override def indexManager: IndexManager = new IndexManager {
    override def createIndex(index: Index): Unit = db.createIndexOnNode(index.labelName.value, index.properties.map(_.value))

    override def dropIndex(index: Index): Unit = {
      val label = index.labelName.value
      val props = index.properties.map(_.value)
      props.foreach(propName => db.dropIndexOnNode(label, propName))
    }

    override def indexes: Array[Index] = {
      val indexMeta = db.getIndexStore.getIndexMeta
      indexMeta.map { case (label, props) => Index(LynxNodeLabel(label), props.map(LynxPropertyKey).toSet) }.toArray
    }
  }

  override def write: WriteTask = _writeTask

  override def nodes(): Iterator[LynxNode] = db.scanAllNodes()

  override def relationships(): Iterator[PathTriple] = db.scanAllRelations().map(
    r => PathTriple(db.getNodeById(r.startNodeId.value).get, r, db.getNodeById(r.endNodeId.value).get)
  )

  val _writeTask: WriteTask = new WriteTask {
    val _nodesBuffer: mutable.Map[NodeId, PandaNode] = mutable.Map()
    val _nodesToDelete: mutable.ArrayBuffer[NodeId] = mutable.ArrayBuffer()
    val _relationshipsBuffer: mutable.Map[RelationId, PandaRelationship] = mutable.Map()
    val _relationshipsToDelete: mutable.ArrayBuffer[RelationId] = mutable.ArrayBuffer()

    private def updateNodes(ids: Iterator[LynxId], update: PandaNode => PandaNode): Iterator[Option[LynxNode]] = {
      ids.map {
        id =>
          val updated = _nodesBuffer.get(id).orElse(db.getNodeById(id.value.asInstanceOf[Long])).map(update)
          updated.foreach(newNode => _nodesBuffer.update(newNode.id, newNode))
          updated
      }
    }

    private def updateRelationships(ids: Iterator[LynxId], update: PandaRelationship => PandaRelationship): Iterator[Option[PandaRelationship]] = {
      ids.map { id =>
        val updated = _relationshipsBuffer.get(id).orElse(db.getRelationById(id.value.asInstanceOf[Long])).map(update)
        updated.foreach(newRel => _relationshipsBuffer.update(newRel.id, newRel))
        updated
      }
    }

    override def createElements[T](nodesInput: Seq[(String, NodeInput)], relationshipsInput: Seq[(String, RelationshipInput)],
                                   onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = {
      val nodesMap = nodesInput.toMap.map {
        case (valueName, input) =>
          val id = db.newNodeId()
          valueName -> PandaNode(NodeId(id), input.labels, input.props.toMap)
      }

      def localNodeRef(ref: NodeInputRef): NodeId = ref match {
        case StoredNodeInputRef(id) => id
        case ContextualNodeInputRef(varname) => nodesMap(varname).id
      }

      val relationshipsMap = relationshipsInput.toMap.map {
        case (valueName, input) =>
          val id = db.newRelationshipId()
          valueName -> PandaRelationship(RelationId(id), localNodeRef(input.startNodeRef), localNodeRef(input.endNodeRef),
            input.types.headOption, input.props.toMap)
      }

      _nodesBuffer ++= nodesMap.map { case (_, node) => (node.id, node) }
      _relationshipsBuffer ++= relationshipsMap.map { case (_, relationship) => (relationship.id, relationship) }

      onCreated(nodesMap.toSeq, relationshipsMap.toSeq)
    }

    override def deleteRelations(ids: Iterator[LynxId]): Unit = {
      ids.foreach(id => {
        _relationshipsBuffer.remove(id)
        _relationshipsToDelete.append(id)
      })
    }

    override def deleteNodes(ids: Seq[LynxId]): Unit = {
      ids.foreach(id => {
        _nodesBuffer.remove(id)
        _nodesToDelete.append(id)
      })
    }

    override def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)],
                                    cleanExistProperties: Boolean): Iterator[Option[LynxNode]] = {
      updateNodes(nodeIds, old => PandaNode(old.id, old.labels, if (cleanExistProperties) Map.empty else old.props ++ data.toMap.mapValues(LynxValue.apply)))
    }

    override def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = {
      updateNodes(nodeIds, old => PandaNode(old.id, (old.labels ++ labels).distinct, old.props))
    }

    override def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)]): Iterator[Option[LynxRelationship]] = {
      updateRelationships(relationshipIds, old => PandaRelationship(old.id, old.startNodeId, old.endNodeId, old.relationType, old.props ++ data.toMap.mapValues(LynxValue.apply)))
    }

    override def setRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = {
      throw new PandaDBException("not support to change relationship type.")
    }

    override def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxNode]] = {
      updateNodes(nodeIds, old => PandaNode(old.id, old.labels, old.props.filterNot(p => data.contains(p._1))))
    }

    override def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = {
      updateNodes(nodeIds, old => PandaNode(old.id, old.labels.filterNot(labels.contains), old.props))
    }

    override def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxRelationship]] = {
      updateRelationships(relationshipIds, old => PandaRelationship(old.id, old.startNodeId, old.endNodeId, old.relationType, old.props.filterNot(p => data.contains(p._1))))
    }

    override def removeRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = {
      throw new PandaDBException("not support remove relationship type.")
    }

    override def commit: Boolean = {
      db.addRelations(_relationshipsBuffer.values.toIterator)
      db.deleteRelations(_relationshipsToDelete.map(_.value).iterator)
      db.addNodes(_nodesBuffer.values.toIterator)
      db.deleteNodes(_nodesToDelete.map(_.value).toIterator)

      _relationshipsBuffer.clear()
      _relationshipsToDelete.clear()
      _nodesBuffer.clear()
      _nodesToDelete.clear()
      true
    }
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = {
    (nodeFilter.labels.nonEmpty, nodeFilter.properties.nonEmpty) match {
      case (false, false) => db.scanAllNodes()
      case (true, false) => {
        val labels = nodeFilter.labels.map(_.value)
        val minLabel = labels.minBy(label => db.getStatistics.getNodeLabelCount(db.getNodeLabelId(label).get))
        db.getNodesByLabel(minLabel, false)
      }
      case (false, true) => db.scanAllNodes().filter(nodeFilter.matches(_))
      case _ => {
        val nodeHasIndex = db.getIndexStore.isNodeHasIndex(nodeFilter)
        if (nodeHasIndex) {
          db.getNodesByIndex(nodeFilter)
        }
        else {
          val labels = nodeFilter.labels.map(_.value)
          val minLabel = labels.minBy(label => db.getStatistics.getNodeLabelCount(db.getNodeLabelId(label).get))
          db.getNodesByLabel(minLabel, false).filter(nodeFilter.matches(_))
        }
      }
    }
  }

  override def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter,
                     direction: SemanticDirection, upperLimit: Option[Int], lowerLimit: Option[Int]): Iterator[PathTriple] = {
    if (upperLimit.isDefined || lowerLimit.isDefined) throw new PandaDBException("not impl relation with length, wait to impl")

    nodes(startNodeFilter).flatMap(node => paths(node.id, relationshipFilter, endNodeFilter, direction))
  }

  private def paths(nodeId: LynxId,
            relationshipFilter: RelationshipFilter,
            endNodeFilter: NodeFilter,
            direction: SemanticDirection): Iterator[PathTriple] = {
    db.getNodeById(nodeId.value.asInstanceOf[Long]).map(
      node => {
        if (relationshipFilter.types.nonEmpty) {
          relationshipFilter.types.map(t => db.getRelationTypeId(t.value)).map(
            _.map(
              relType =>
                direction match {
                  case SemanticDirection.INCOMING => db.findInRelations(node.id.value, Some(relType)).map(r => (node, r, r.startNodeId))
                  case SemanticDirection.OUTGOING => db.findOutRelations(node.id.value, Some(relType)).map(r => (node, r, r.endNodeId))
                  case SemanticDirection.BOTH => db.findInRelations(node.id.value, Some(relType)).map(r => (node, r, r.startNodeId)) ++
                    db.findOutRelations(node.id.value, Some(relType)).map(r => (node, r, r.endNodeId))
                }
            ).getOrElse(Iterator.empty)
          )
        } else {
          Seq(direction match {
            case SemanticDirection.INCOMING => db.findInRelations(node.id.value).map(r => (node, r, r.startNodeId))
            case SemanticDirection.OUTGOING => db.findOutRelations(node.id.value).map(r => (node, r, r.endNodeId))
            case SemanticDirection.BOTH => db.findInRelations(node.id.value).map(r => (node, r, r.startNodeId)) ++
              db.findOutRelations(node.id.value).map(r => (node, r, r.endNodeId))
          })
        }
      }
    )
      .getOrElse(Iterator.empty)
      .reduce(_ ++ _)
      .map(p => PathTriple(p._1, db.getRelationById(p._2.id.value).orNull, db.getNodeById(p._3.value).orNull))
      .filter(trip => endNodeFilter.matches(trip.endNode))
  }

  override def expand(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] = {
    direction match {
      case SemanticDirection.INCOMING => db.findInRelations(nodeId.value.asInstanceOf[Long]).map(r => {
        val toNode = db.getNodeById(r.endNodeId.value).get
        val fromNode = db.getNodeById(r.startNodeId.value).get
        PathTriple(toNode, r, fromNode)
      })
      case SemanticDirection.OUTGOING => db.findOutRelations(nodeId.value.asInstanceOf[Long]).map(r => {
        val toNode = db.getNodeById(r.endNodeId.value).get
        val fromNode = db.getNodeById(r.startNodeId.value).get
        PathTriple(fromNode, r, toNode)
      })
      case SemanticDirection.BOTH => db.findInRelations(nodeId.value.asInstanceOf[Long]).map(r => {
        val toNode = db.getNodeById(r.endNodeId.value).get
        val fromNode = db.getNodeById(r.startNodeId.value).get
        PathTriple(toNode, r, fromNode)
      }) ++
        db.findOutRelations(nodeId.value.asInstanceOf[Long]).map(r => {
          val toNode = db.getNodeById(r.endNodeId.value).get
          val fromNode = db.getNodeById(r.startNodeId.value).get
          PathTriple(fromNode, r, toNode)
        })
    }
  }

  override def expand(nodeId: LynxId, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
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

    private def expand(nodeId: LynxId, direction: SemanticDirection, relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
      val typeId = {
        relationshipFilter.types match {
          case Seq() => None
          case _ => db.getRelationTypeId(relationshipFilter.types.head.value)
        }
      }
      if (typeId.isEmpty && relationshipFilter.types.nonEmpty) {
        Iterator.empty
      } else {
        direction match {
          case SemanticDirection.INCOMING => db.findInRelations(nodeId.value.asInstanceOf[Long], typeId).map(r => {
            val toNode = db.getNodeById(r.endNodeId.value).get
            val fromNode = db.getNodeById(r.startNodeId.value).get
            PathTriple(toNode, r, fromNode)
          })

          case SemanticDirection.OUTGOING => db.findOutRelations(nodeId.value.asInstanceOf[Long], typeId).map(r => {
            val toNode = db.getNodeById(r.endNodeId.value).get
            val fromNode = db.getNodeById(r.startNodeId.value).get
            PathTriple(fromNode, r, toNode)
          })
          case SemanticDirection.BOTH => db.findInRelations(nodeId.value.asInstanceOf[Long], typeId).map(r => {
            val toNode = db.getNodeById(r.endNodeId.value).get
            val fromNode = db.getNodeById(r.startNodeId.value).get
            PathTriple(toNode, r, fromNode)
          }) ++
            db.findOutRelations(nodeId.value.asInstanceOf[Long], typeId).map(r => {
              val toNode = db.getNodeById(r.endNodeId.value).get
              val fromNode = db.getNodeById(r.startNodeId.value).get
              PathTriple(fromNode, r, toNode)
            })
        }
      }
    }

  //  override def mergeNode(nodeFilter: NodeFilter, forceToCreate: Boolean, tx: Option[LynxTransaction]): LynxNode = {
  //    val props = nodeFilter.properties.map(kv => (kv._1, kv._2.value))
  //    if (forceToCreate){
  //      val id = db.addNode(props, nodeFilter.labels:_*)
  //      db.getNodeById(id).get
  //    }
  //    else {
  //      val n = nodes(nodeFilter, tx)
  //      if (n.nonEmpty) n.next()
  //      else db.getNodeById(db.addNode(props, nodeFilter.labels:_*)).get
  //    }
  //  }
  //
  //  override def mergeRelationship(relationshipFilter: RelationshipFilter, leftNode: LynxNode, rightNode: LynxNode, direction: SemanticDirection, forceToCreate: Boolean, tx: Option[LynxTransaction]): PathTriple = {
  //    val props = relationshipFilter.properties.map(kv => (kv._1, kv._2.value))
  //    val id = db.addRelation(relationshipFilter.types.head, leftNode.id, rightNode.id, props)
  //    PathTriple(leftNode, db.getRelation(id).get, rightNode)
  //  }
  //
  //  override def createIndex(labelName: LabelName, properties: List[PropertyKeyName], tx: Option[LynxTransaction]): Unit = {
  //    db.createIndexOnNode(labelName.name, properties.map(p => p.name).toSet)
  //  }
  //
  //  override def getIndexes(tx: Option[LynxTransaction]): Array[(LabelName, List[PropertyKeyName])] = {
  ////    db.getIndexes()
  //    ???
  //  }
}
