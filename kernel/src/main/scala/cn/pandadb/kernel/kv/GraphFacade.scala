package cn.pandadb.kernel.kv

import com.typesafe.scalalogging.LazyLogging
import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.{NameStore, Statistics}
import cn.pandadb.kernel.store._
import org.grapheco.lynx.{CallableProcedure, ContextualNodeInputRef, CypherRunner, GraphModel, LynxId, LynxNode, LynxRelationship, LynxResult, LynxValue, NodeFilter, NodeInput, NodeInputRef, PathTriple, RelationshipFilter, RelationshipInput, StoredNodeInputRef}
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName, SemanticDirection}


class GraphFacade(nodeStore: NodeStoreSPI,
                  relationStore: RelationStoreSPI,
                  indexStore: IndexStoreAPI,
                  statistics: Statistics,
                  onClose: => Unit
                 ) extends LazyLogging with GraphService with GraphModel{

  val runner = new CypherRunner(this)

  init()

  override def getIndexes(): Array[(LabelName, List[PropertyKeyName])] = {
    ???
  }

  override def cypher(query: String, parameters: Map[String, Any] = Map.empty): LynxResult = {
    runner.compile(query)
    runner.run(query, parameters)
  }

  override def close(): Unit = {
    statistics.flush()
    statistics.close()
    nodeStore.close()
    relationStore.close()
    indexStore.close()
    onClose
  }

  private def nodeLabelNameMap(name: String): Int = nodeStore.getLabelId(name)

  private def nodePropNameMap(name: String): Int = nodeStore.getPropertyKeyId(name)

  private def relTypeNameMap(name: String): Int = relationStore.getRelationTypeId(name)

  private def relPropNameMap(name: String): Int = relationStore.getPropertyKeyId(name)

  override def addNode(nodeProps: Map[String, Any], labels: String*): Id = {
    addNode(None, labels, nodeProps)
  }

  private def addNode(id: Option[Long], labels: Seq[String], nodeProps: Map[String, Any]): Id = {
    val nodeId = id.getOrElse(nodeStore.newNodeId())
    val labelIds = nodeStore.getLabelIds(labels.toSet).toArray
    val props = nodeProps.map{
      v => ( nodePropNameMap(v._1),v._2)
    }
    nodeStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, props))
    statistics.increaseNodeCount(1) // TODO batch
    labelIds.foreach(statistics.increaseNodeLabelCount(_, 1))
    // index
    labelIds.map(indexStore.getIndexIdByLabel).foreach(
      _.foreach{
        propsIndex =>
          if (propsIndex.props.length<=1){
            indexStore.insertIndexRecord(propsIndex.indexId, props.getOrElse(propsIndex.props.head, null), nodeId)
          }else {
            // TODO combined index
          }
          statistics.increaseIndexPropertyCount(propsIndex.indexId, 1)
    })
    nodeId
  }

  override def addRelation(label: String, from: Id, to: Id, relProps: Map[String, Any]): Id = {
    addRelation(None, label, from, to, relProps)
  }

  private def addRelation(id: Option[Long], label: String, from: Long, to: Long, relProps: Map[String, Any]): Id = {
    val rid = id.getOrElse(relationStore.newRelationId())
    val labelId = relTypeNameMap(label)
    val props = relProps.map{
      v => ( relPropNameMap(v._1),v._2)
    }
    val rel = new StoredRelationWithProperty(rid, from, to, labelId, props)
    //TODO: transaction safe
    relationStore.addRelation(rel)
    statistics.increaseRelationCount(1) // TODO batch , index statistic
    statistics.increaseRelationTypeCount(labelId, 1)
    rid
  }

  override def deleteNode(id: Id): Unit = {
    nodeStore.getNodeById(id).foreach{
      node =>
        nodeStore.deleteNode(node.id)
        statistics.decreaseNodes(1)
        node.labelIds.foreach(statistics.decreaseNodeLabelCount(_, 1))
        node.labelIds.map(indexStore.getIndexIdByLabel).foreach(
          _.foreach{
            propsIndex =>
              if (propsIndex.props.length<=1){
                indexStore.deleteIndexRecord(propsIndex.indexId, node.properties.getOrElse(propsIndex.props.head, null), node.id)
              }else {
                // TODO combined index
              }
              statistics.decreaseIndexPropertyCount(propsIndex.indexId, 1)
          })
    }
  }

  override def deleteRelation(id: Id): Unit = {
    relationStore.getRelationById(id).foreach{
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

  def createNodePropertyIndex(nodeLabel: String, propertyNames: Set[String]): Int = {
    val labelId = nodeLabelNameMap(nodeLabel)
    val propNameIds = propertyNames.map(nodePropNameMap)
    indexStore.createIndex(labelId, propNameIds.toArray)
  }

  def writeNodeIndexRecord(indexId: Int, nodeId: Long, propertyValue: Any): Unit = {
    indexStore.insertIndexRecord(indexId, propertyValue, nodeId)
  }

  override def nodeSetProperty(id: Id, key: String, value: Any): Unit =
    nodeStore.nodeSetProperty(id, nodePropNameMap(key), value)

  override def nodeRemoveProperty(id: Id, key: String): Unit =
    nodeStore.nodeRemoveProperty(id, nodePropNameMap(key))

  override def nodeAddLabel(id: Id, label: String): Unit =
    nodeStore.nodeAddLabel(id, nodeLabelNameMap(label))

  override def nodeRemoveLabel(id: Id, label: String): Unit =
    nodeStore.nodeRemoveLabel(id, nodeLabelNameMap(label))

  override def relationSetProperty(id: Id, key: String, value: Any): Unit =
    relationStore.relationSetProperty(id, relPropNameMap(key), value)

  override def relationRemoveProperty(id: Id, key: String): Unit =
    relationStore.relationRemoveProperty(id, relPropNameMap(key))

  override def relationAddLabel(id: Id, label: String): Unit = ???

  override def relationRemoveLabel(id: Id, label: String): Unit = ???
  
  override def createIndexOnNode(label: String, props: Set[String]): Unit = {
    val labelId = nodeLabelNameMap(label)
    val propsId = props.map(nodePropNameMap).toArray.sorted
    val indexId = indexStore.createIndex(labelId, propsId)
    if(propsId.length == 1) {
      indexStore.insertIndexRecordBatch(
        indexId,
        nodeStore.getNodesByLabel(labelId).map{
          node =>
            (node.properties.getOrElse(propsId(0),null), node.id)
        }
      )
      statistics.setIndexPropertyCount(indexId, nodeStore.getNodesByLabel(labelId).length)
    } else {
      // TODO combined index
    }
  }

  override def createIndexOnRelation(typeName: String, props: Set[String]): Unit = {
    val typeId = relTypeNameMap(typeName)
    val propsId = props.map(relPropNameMap).toArray.sorted
    val indexId = indexStore.createIndex(typeId, propsId)
    if(propsId.length == 1) {
      indexStore.insertIndexRecordBatch(
        indexId,
        relationStore.getRelationIdsByRelationType(typeId)
          .map(relationStore.getRelationById(_).get)
          .map{
          rel =>
            (rel.properties.getOrElse(propsId(0),null), rel.id)
        }
      )
      statistics.setIndexPropertyCount(indexId, relationStore.getRelationIdsByRelationType(typeId).length)
    } else {
      // TODO combined index
    }
  }

  override def createIndex(labelName: LabelName, properties: List[PropertyKeyName]): Unit = {
    val l=labelName.name
    val ps = properties.map(p=>p.name).toSet
    createIndexOnNode(l, ps)
    createIndexOnRelation(l, ps)
  }

  def refresh():Unit = {
    statistics.nodeCount = nodeStore.nodesCount
    logger.info(s"node count: ${statistics.nodeCount}")
    statistics.relationCount = relationStore.relationCount
    logger.info(s"relation count: ${statistics.relationCount}")
    nodeStore.allLabelIds().foreach{
      l =>
        statistics.setNodeLabelCount(l, nodeStore.getNodeIdsByLabel(l).length)
        logger.info(s"label ${l} count: ${statistics.getNodeLabelCount(l)}")
    }
    relationStore.allRelationTypeIds().foreach{
      t =>
        statistics.setRelationTypeCount(t,
          relationStore.getRelationIdsByRelationType(t).length)
        logger.info(s"type ${t} count: ${statistics.getRelationTypeCount(t)}")
    }
    indexStore.allIndexId.foreach{
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
      node.properties.map(kv=>(nodeStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq:_*)
  }

  protected def mapRelation(rel: StoredRelation): PandaRelationship = {
    PandaRelationship(rel.id,
      rel.from, rel.to,
      relationStore.getRelationTypeName(rel.typeId),
      rel.properties.map(kv=>(relationStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq:_*)
  }

  //TODO need props?
  def rels(includeStartNodes: Boolean, includeEndNodes: Boolean): Iterator[(PandaRelationship, Option[PandaNode], Option[PandaNode])] = {
    relationStore.allRelations(withProperty = true).map(mapRelation).map{ rel =>
      (rel,
        if (includeStartNodes) nodeAt(rel.startNodeId) else None,
        if (includeEndNodes) nodeAt(rel.endNodeId) else None)
    }
  }

  override def nodes(): Iterator[PandaNode] = nodeStore.allNodes().map(mapNode)

  def nodeAt(id: LynxId): Option[PandaNode] = nodeAt(id.value.asInstanceOf[Long])

  def nodeAt(id: Long): Option[PandaNode] = nodeStore.getNodeById(id).map(mapNode)

  def nodeAt(id: Long, label: String): Option[PandaNode] =
    nodeStore.getNodeById(id, Option(nodeLabelNameMap(label))).map(mapNode)

  def nodeHasLabel(id: Long, label: String): Boolean = nodeStore.hasLabel(id, nodeLabelNameMap(label))

  def relationAt(id: Long): Option[PandaRelationship] = relationStore.getRelationById(id).map(mapRelation)

  def relationAt(id: LynxId): Option[PandaRelationship] = relationAt(id.value.asInstanceOf[Long])



  def rels(types: Seq[String],
                    labels1: Seq[String],
                    labels2: Seq[String],
                    includeStartNodes: Boolean,
                    includeEndNodes: Boolean): Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] ={
    val includeProperty = false
    rels(types, labels1, labels2, includeProperty, includeStartNodes, includeEndNodes)
  }

  def rels(types: Seq[String],
                    labels1: Seq[String],
                    labels2: Seq[String],
                    includeProperty: Boolean,
                    includeStartNodes: Boolean,
                    includeEndNodes: Boolean): Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] = {

    val label1Id = labels1.map(nodeLabelNameMap).sorted
    val label2Id = labels2.map(nodeLabelNameMap).sorted
    val typeIds  = types.map(relTypeNameMap).sorted

    (label1Id.nonEmpty, typeIds.nonEmpty, label2Id.nonEmpty) match {
      case (_, true, true)    =>  // [nodesWithLabel1]-[relWithTypes]-[nodesWithLabel2]
        // TODO if types is small
        typeIds.map(
          typeId =>
            getNodesByLabels(labels1, exact = false).flatMap{
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
        ).reduce(_++_)
      case (_, false, true)   => // [nodesWithLabel1]-[rels]-[nodesWithLabel2]
        getNodesByLabels(labels1, exact = false).flatMap{
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
      case (_, true, false)   => // [nodesWithLabel1]-[relsWithTypes]-[nodes]
        typeIds.map(
          typeId =>
            getNodesByLabels(labels1, exact = false).flatMap{
              startNode =>
                relationStore
                  .findOutRelations(startNode.id, Some(typeId))
                  .map(rel => (
                    if (includeProperty) relationAt(rel.id).orNull else mapRelation(rel),
                    if (includeStartNodes) Some(mapNode(startNode)) else None,
                    if (includeEndNodes) nodeAt(rel.to) else None
                  ))
            }
        ).reduce(_++_)
      case (true, false, false)  =>  // [nodesWithLabel1]-[allRel]-[allNodes]
        getNodesByLabels(labels1, exact = false).flatMap{
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
          .map(rel=>(
            mapRelation(rel),
            if (includeStartNodes) nodeAt(rel.from) else None,
            if (includeEndNodes) nodeAt(rel.to) else None
          ))
    }
  }

  def nodes(labels: Seq[String], exact: Boolean): Iterator[PandaNode] = getNodesByLabels(labels, exact).map(mapNode)

  def getNodesByLabels(labels: Seq[String], exact: Boolean): Iterator[StoredNode] = {
    if (labels.isEmpty) {
      nodeStore.allNodes()
    } else if(labels.size == 1){
      val labelId = labels.map(nodeLabelNameMap).head
      nodeStore.getNodesByLabel(labelId)
    } else {
      //TODO statistics choose one min count
      val labelIds = labels.map(nodeLabelNameMap).sorted
      val label = nodeLabelNameMap(labels.head)
      nodeStore.getNodesByLabel(label).filter {
        if (exact)
          _.labelIds.sorted.toSeq == labelIds
        else
          _.labelIds.sorted.containsSlice(labelIds)
      }
    }
  }

  def filterNodes(p: PandaNode, properties: Map[String, LynxValue]): Boolean =
    properties.forall(x => p.properties.exists(x.equals))

  def hasIndex(labels: Set[String], propertyNames: Set[String]): Option[(Int, String, Set[String], Long)] = {
    val propertyIds = propertyNames.map(nodeStore.getPropertyKeyId).toArray.sorted
    val labelIds = labels.map(nodeLabelNameMap)
    val combinations = propertyIds.toSet.subsets().drop(1)
    val indexes = labelIds.flatMap(
      label => combinations.flatMap (
        props => indexStore.getIndexId(label, props.toArray).map((_, label, props, 0L))
      )
    )
    val res = indexes.flatMap(p => statistics.getIndexPropertyCount(p._1).map((p._1, p._2, p._3 ,_)))
    res.headOption
      .map(h => res.minBy(_._4))
      .orElse(indexes.headOption)
      .map(res => (res._1, nodeStore.getLabelName(res._2).get, res._3.map(nodeStore.getPropertyKeyName(_).get), res._4))
  }

  def findNodeByIndex(indexId: Int, value: Any): Iterator[StoredNodeWithProperty] =
    indexStore.find(indexId, value).flatMap(nodeStore.getNodeById(_))

  override def getProcedure(prefix: List[String], name: String): Option[CallableProcedure] = super.getProcedure(prefix, name) //todo when procedure is need

  override def paths(startNodeFilter: NodeFilter,
                     relationshipFilter: RelationshipFilter,
                     endNodeFilter: NodeFilter,
                     direction: SemanticDirection): Iterator[PathTriple] =
    nodes(startNodeFilter).flatMap(node => paths(node.id,relationshipFilter,endNodeFilter, direction))


   def paths(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] =
    nodeAt(nodeId).map(
        n=>
          direction match {
            case SemanticDirection.INCOMING => relationStore.findInRelations(n.longId).map(r => (n, r, r.from))
            case SemanticDirection.OUTGOING => relationStore.findOutRelations(n.longId).map(r => (n, r, r.to))
            case SemanticDirection.BOTH     => relationStore.findInRelations(n.longId).map(r => (n, r, r.from)) ++
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
      node =>
        relationshipFilter.types.map(relTypeNameMap).map(
          relType =>
            direction match {
              case SemanticDirection.INCOMING => relationStore.findInRelations(node.longId, Some(relType)).map(r => (node, r, r.from))
              case SemanticDirection.OUTGOING => relationStore.findOutRelations(node.longId, Some(relType)).map(r => (node, r, r.to))
              case SemanticDirection.BOTH => relationStore.findInRelations(node.longId, Some(relType)).map(r => (node, r, r.from)) ++
                relationStore.findOutRelations(node.longId, Some(relType)).map(r => (node, r, r.to))
            }
        )
    )
      .getOrElse(Iterator.empty)
      .reduce(_ ++ _)
      .map(p => PathTriple(p._1, relationAt(p._2.id).orNull, nodeAt(p._3).orNull))
      .filter(trip => endNodeFilter.matches(trip.endNode))
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[PandaNode] = {
    (nodeFilter.labels.nonEmpty, nodeFilter.properties.nonEmpty) match {
      case (false, false) => allNodes().iterator.map(mapNode)
      case (true, false)  => nodes(nodeFilter.labels, exact = false)
      case (false, true)  => allNodes().iterator.map(mapNode).filter(filterNodes(_, nodeFilter.properties))
      case (true, true)   =>
        hasIndex(nodeFilter.labels.toSet, nodeFilter.properties.keys.toSet)
          .map {
            indexInfo => // fixme ------------------------------------only get one prop ↓
              logger.info("seek index: " + indexInfo)
              findNodeByIndex(indexInfo._1, nodeFilter.properties.getOrElse(indexInfo._3.head, null).value)
                .map(mapNode)
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

  override def relationships(): Iterator[PathTriple] =
    allRelations().toIterator.map(rel => PathTriple(nodeAt(rel.from).get, mapRelation(rel), nodeAt(rel.to).get))

  override def createElements[T](nodesInput: Seq[(String, NodeInput)], relsInput: Seq[(String, RelationshipInput)], onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = {
    val nodesMap: Seq[(String, PandaNode)] = nodesInput.map(x => {
      val (varname, input) = x
      val id = nodeStore.newNodeId()
      varname-> PandaNode(id, input.labels, input.props:_*)
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
      varname -> PandaRelationship(relationStore.newRelationId(), nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption, input.props:_*)
    }
    )

    nodesMap.foreach{
      node => addNode(Some(node._2.longId), node._2.labels,  node._2.props.toMap.mapValues(_.value))
    }

    relsMap.foreach{
      rel => addRelation(rel._2.relationType.get, rel._2.startId, rel._2.endId, rel._2.properties.mapValues(_.value))
    }

    onCreated(nodesMap, relsMap)
  }
}
