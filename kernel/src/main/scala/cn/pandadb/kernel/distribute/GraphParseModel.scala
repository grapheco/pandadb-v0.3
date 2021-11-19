package cn.pandadb.kernel.distribute

import cn.pandadb.kernel.kv.value.ValueMappings
import cn.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.grapheco.lynx.{ContextualNodeInputRef, LynxId, LynxNode, LynxRelationship, LynxTransaction, LynxValue, NodeFilter, NodeInput, NodeInputRef, PathTriple, RelationshipFilter, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.cypherplus.{Blob, GraphModelPlus, SemanticComparator}
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName, SemanticDirection}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-19 13:36
 */
class GraphParseModel(db: DistributedGraphService) extends GraphModelPlus{
  implicit def LynxId2Long(lid: LynxId): Long = lid.value.asInstanceOf[Long]

  override def nodes(tx: Option[LynxTransaction]): Iterator[LynxNode] = db.scanAllNode()

  override def setNodeProperty(nodeId: LynxId, data: Array[(String, Any)], cleanExistProperties: Boolean, tx: Option[LynxTransaction]): Option[LynxNode] = {
    db.getNode(nodeId).map(node => {
      if (cleanExistProperties) node.properties.foreach(kv => db.nodeRemoveProperty(nodeId, kv._1)) // TODO: optimize
      data.foreach(kv => db.nodeSetProperty(nodeId, kv._1, kv._2))
      db.getNode(nodeId, node.labels.head).get
    })
  }

  override def addNodeLabels(nodeId: LynxId, labels: Array[String], tx: Option[LynxTransaction]): Option[LynxNode] = {
    labels.foreach(label => db.nodeAddLabel(nodeId, label))
    db.getNode(nodeId)
  }

  override def removeNodeProperty(nodeId: LynxId, propertyKeyNames: Array[String], tx: Option[LynxTransaction]): Option[LynxNode] = {
    propertyKeyNames.foreach(key => db.nodeRemoveProperty(nodeId, key))
    db.getNode(nodeId)
  }

  override def removeNodeLabels(nodeId: LynxId, labels: Array[String], tx: Option[LynxTransaction]): Option[LynxNode] = {
    labels.foreach(label => db.nodeRemoveLabel(nodeId, label))
    db.getNode(nodeId)
  }

  override def copyNode(srcNode: LynxNode, maskNode: LynxNode, tx: Option[LynxTransaction]): Seq[LynxValue] = {
    val nodeId = srcNode.id
    val property = maskNode.asInstanceOf[PandaNode].properties.map(kv => (kv._1, kv._2.value))
    db.deleteNode(nodeId)
    db.addNode(nodeId, property, maskNode.labels:_*)
    Seq(db.getNode(nodeId).get)
  }

  override def mergeNode(nodeFilter: NodeFilter, forceToCreate: Boolean, tx: Option[LynxTransaction]): LynxNode = {
    val props = nodeFilter.properties.map(kv => (kv._1, kv._2.value))
    if (forceToCreate){
      val id = db.addNode(props, nodeFilter.labels:_*)
      db.getNode(id).get
    }
    else {
      val n = nodes(nodeFilter, tx)
      if (n.nonEmpty) n.next()
      else db.getNode(db.addNode(props, nodeFilter.labels:_*)).get
    }
  }

  override def deleteFreeNodes(nodesIDs: Seq[LynxId], tx: Option[LynxTransaction]): Unit = {
    db.deleteNodes(nodesIDs.map(f => f.value.asInstanceOf[Long]).toIterator)
  }

  override def relationships(tx: Option[LynxTransaction]): Iterator[PathTriple] = {
    db.scanAllRelations().map(relation => PathTriple(db.getNode(relation.startId).get, relation, db.getNode(relation.endId).get))
  }

  override def deleteRelation(id: LynxId, tx: Option[LynxTransaction]): Unit = {
    db.deleteRelation(id)
  }

  override def deleteRelations(ids: Iterator[LynxId], tx: Option[LynxTransaction]): Unit = {
    // TODO: batch delete
    ids.foreach(db.deleteRelation(_))
  }

  override def pathsWithLength(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection, length: Option[Option[expressions.Range]], tx: Option[LynxTransaction]): Iterator[Seq[PathTriple]] = ???

  override def mergeRelationship(relationshipFilter: RelationshipFilter, leftNode: LynxNode, rightNode: LynxNode, direction: SemanticDirection, forceToCreate: Boolean, tx: Option[LynxTransaction]): PathTriple = {
    val props = relationshipFilter.properties.map(kv => (kv._1, kv._2.value))
    val id = db.addRelation(relationshipFilter.types.head, leftNode.id, rightNode.id, props)
    PathTriple(leftNode, db.getRelation(id).get, rightNode)
  }

  override def filterNodesWithRelations(nodesIDs: Seq[LynxId], tx: Option[LynxTransaction]): Seq[LynxId] = {
    nodesIDs.filter(nid => {
      db.findOutRelations(nid).nonEmpty || db.findInRelations(nid).nonEmpty
    })
  }

  override def deleteRelationsOfNodes(nodesIDs: Seq[LynxId], tx: Option[LynxTransaction]): Unit = {
    nodesIDs.foreach(nid => {
      db.findOutRelations(nid).foreach(rel => db.deleteRelation(rel.id))
      db.findInRelations(nid).foreach(rel => db.deleteRelation(rel.id))
    })
  }

  override def setRelationshipProperty(triple: Seq[LynxValue], data: Array[(String, Any)], tx: Option[LynxTransaction]): Option[Seq[LynxValue]] = {
    val relId = triple(1).asInstanceOf[LynxRelationship].id
    data.foreach(kv => db.relationSetProperty(relId, kv._1, kv._2))
    val relationship = db.getRelation(relId)
    if (relationship.isDefined) {
      Option(Seq(triple.head, relationship.get, triple(2)))
    }
    else None
  }

  override def removeRelationshipProperty(triple: Seq[LynxValue], data: Array[String], tx: Option[LynxTransaction]): Option[Seq[LynxValue]] = {
    val relId = triple(1).asInstanceOf[LynxRelationship].id
    data.foreach(key => db.relationRemoveProperty(relId, key))
    val relationship = db.getRelation(relId)
    if (relationship.isDefined) Option(Seq(triple.head, relationship.get, triple(2)))
    else None
  }

  override def setRelationshipTypes(triple: Seq[LynxValue], labels: Array[String], tx: Option[LynxTransaction]): Option[Seq[LynxValue]] = ???

  override def removeRelationshipType(triple: Seq[LynxValue], labels: Array[String], tx: Option[LynxTransaction]): Option[Seq[LynxValue]] = ???

  override def createElements[T](nodesInput: Seq[(String, NodeInput)], relsInput: Seq[(String, RelationshipInput)], onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T, tx: Option[LynxTransaction]): T = {
    val nodesMap: Seq[(String, PandaNode)] = nodesInput.map(x => {
      val (varname, input) = x
      val id = db.newNodeId()
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
      varname -> PandaRelationship(db.newRelationshipId(), nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption, input.props: _*)
    }
    )

    nodesMap.foreach {
      node => {
        val props = node._2.props.toMap.mapValues(ValueMappings.lynxValueMappingToScala)
        db.addNode(node._2.longId, props, node._2.labels:_*)
      }
    }

    relsMap.foreach {
      rel => {
        db.addRelation(rel._2._id, rel._2.relationType.get, rel._2.startId, rel._2.endId, rel._2.properties.mapValues(ValueMappings.lynxValueMappingToScala))
      }
    }

    onCreated(nodesMap, relsMap)
  }

  override def createIndex(labelName: LabelName, properties: List[PropertyKeyName], tx: Option[LynxTransaction]): Unit = ???

  override def getIndexes(tx: Option[LynxTransaction]): Array[(LabelName, List[PropertyKeyName])] = ???

  override def getSubProperty(value: LynxValue, propertyKey: String): LynxValue = ???

  override def getSemanticComparator(algoName: Option[String]): SemanticComparator = ???

  override def getInternalBlob(bid: String): Blob = ???

  override def estimateNodeLabel(labelName: String): Long = 1

  override def estimateNodeProperty(labelName: String, propertyName: String, value: AnyRef): Long = 1

  override def estimateRelationship(relType: String): Long = 1

  // override
  override def getAllNodeCount(tx: Option[LynxTransaction]): Long = 1

  override def getAllRelationshipsCount(tx: Option[LynxTransaction]): Long = 1

  override def relationships(relationshipFilter: RelationshipFilter, tx: Option[LynxTransaction]): Iterator[PathTriple] = {
    super.relationships(relationshipFilter, tx)
  }

  override def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection, tx: Option[LynxTransaction]): Iterator[PathTriple] = super.paths(startNodeFilter, relationshipFilter, endNodeFilter, direction, tx)

  override def expand(nodeId: LynxId, direction: SemanticDirection, tx: Option[LynxTransaction]): Iterator[PathTriple] = super.expand(nodeId, direction, tx)

  override def expand(nodeId: LynxId, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection, tx: Option[LynxTransaction]): Iterator[PathTriple] = super.expand(nodeId, relationshipFilter, endNodeFilter, direction, tx)

  override def nodes(nodeFilter: NodeFilter, tx: Option[LynxTransaction]): Iterator[PandaNode] = {
    (nodeFilter.labels.nonEmpty, nodeFilter.properties.nonEmpty) match {
      case (false, false) => db.scanAllNode()
      case (true, false) => db.getNodesByLabel(nodeFilter.labels, false)
      case _ => {
        // TODO: check is exists index, then go index or not
        ???
      }
    }
  }
}
