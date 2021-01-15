package cn.pandadb.kernel.kv

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.{NameStore, Statistics}
import cn.pandadb.kernel.store._
import org.apache.logging.log4j.scala.Logging
import org.grapheco.lynx.{ContextualNodeInputRef, CypherRunner, GraphModel, LynxId, LynxNode, LynxRelationship, LynxResult, LynxValue, NodeInput, NodeInputRef, RelationshipInput, StoredNodeInputRef}


class GraphFacadeWithPPD( nodeStore: NodeStoreSPI,
                          relationStore: RelationStoreSPI,
                          indexStore: IndexStoreAPI,
                          statistics: Statistics,
                          onClose: => Unit
                 ) extends Logging with GraphService with GraphModel{

  val runner = new CypherRunner(this)

  init()

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
    statistics.close()
    onClose
  }
  
  def nodeLabelNameMap(name: String): Int = nodeStore.getLabelId(name)
  
  def nodePropNameMap(name: String): Int = nodeStore.getPropertyKeyId(name)
  
  def relTypeNameMap(name: String): Int = relationStore.getRelationTypeId(name)
  
  def relPropNameMap(name: String): Int = relationStore.getPropertyKeyId(name)

  override def addNode(nodeProps: Map[String, Any], labels: String*): this.type = {
    addNode(nodeStore.newNodeId(), labels, nodeProps)
  }

  def addNode(nodeId:Long, labels: Seq[String], nodeProps: Map[String, Any]): this.type = {
    val nodeId = nodeStore.newNodeId()
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
    this
  }

  def addNode2(nodeProps: Map[String, Any], labels: String*): Long = {
    val nodeId = nodeStore.newNodeId()
    val labelIds = nodeStore.getLabelIds(labels.toSet).toArray
    val props = nodeProps.map{
      v => ( nodePropNameMap(v._1),v._2)
    }
    nodeStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, props))
    nodeId
  }


  override def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]):this.type = {
    val rid = relationStore.newRelationId()
    val labelId = relTypeNameMap(label)
    val props = relProps.map{
      v => ( relPropNameMap(v._1),v._2)
    }
    val rel = new StoredRelationWithProperty(rid, from, to, labelId, props)
    //TODO: transaction safe
    relationStore.addRelation(rel)
    statistics.increaseRelationCount(1) // TODO batch , index statistic
    statistics.increaseRelationTypeCount(labelId, 1)
    this
  }

  override def deleteNode(id: Id): this.type = {
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
    this
  }

  override def deleteRelation(id: Id): this.type = {
    relationStore.getRelationById(id).foreach{
      rel =>
        relationStore.deleteRelation(rel.id)
        statistics.decreaseRelations(1)
        statistics.setRelationTypeCount(rel.typeId, 1)
    }
    this
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

  def refresh():Unit = {
    statistics.nodeCount = nodeStore.nodesCount
    println(s"node count: ${statistics.nodeCount}")
    statistics.relationCount = relationStore.relationCount
    println(s"relation count: ${statistics.relationCount}")
    nodeStore.allLabelIds().foreach{
      l =>
        statistics.setNodeLabelCount(l, nodeStore.getNodeIdsByLabel(l).length)
        println(s"label ${l} count: ${statistics.getNodeLabelCount(l)}")
    }
    relationStore.allRelationTypeIds().foreach{
      t =>
        statistics.setRelationTypeCount(t,
          relationStore.getRelationIdsByRelationType(t).length)
        println(s"type ${t} count: ${statistics.getRelationTypeCount(t)}")
    }
    indexStore.allIndexId.foreach{
      meta =>
        statistics.setIndexPropertyCount(meta.indexId,
          indexStore.findByPrefix(ByteUtils.intToBytes(meta.indexId)).length)
        println(s"index ${meta.indexId} count: ${statistics.getIndexPropertyCount(meta.indexId)}")
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
  override def rels(includeStartNodes: Boolean, includeEndNodes: Boolean): Iterator[(PandaRelationship, Option[PandaNode], Option[PandaNode])] = {
    relationStore.allRelations().map(mapRelation).map{ rel =>
      (rel,
        if (includeStartNodes) nodeAt(rel.startNodeId) else None,
        if (includeEndNodes) nodeAt(rel.endNodeId) else None)
    }
  }

  override def nodes(): Iterator[PandaNode] = nodeStore.allNodes().map(mapNode)

  override def nodeAt(id: LynxId): Option[PandaNode] = nodeAt(id.value.asInstanceOf[Long])

  def nodeAt(id: Long): Option[PandaNode] = nodeStore.getNodeById(id).map(mapNode)

  override def createElements[T](nodes: Array[(Option[String], NodeInput)], rels: Array[(Option[String], RelationshipInput)], onCreated: (Map[Option[String], LynxNode], Map[Option[String], LynxRelationship]) => T): T = {
    val nodesMap: Map[NodeInput, (Option[String], PandaNode)] = nodes.map(x => {
      val (varname, input) = x
      val id = nodeStore.newNodeId()
      input -> (varname, PandaNode(id, input.labels, input.props:_*))
    }).toMap

    def nodeId(ref: NodeInputRef): Long = {
      ref match {
        case StoredNodeInputRef(id) => id.value.asInstanceOf[Long]
        case ContextualNodeInputRef(node) => nodesMap(node)._2.longId
      }
    }

    val relsMap: Array[(Option[String], PandaRelationship)] = rels.map(x => {
      val (varname, input) = x
      varname -> PandaRelationship(relationStore.newRelationId(), nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption)
    }
    )

    nodesMap.values.foreach{
      node => addNode(node._2.props.toMap.mapValues(_.value), node._2.labels: _*)
    }

    relsMap.foreach{
      rel => addRelation(rel._2.relationType.get, rel._2.startId, rel._2.endId, rel._2.properties.mapValues(_.value))
    }

    onCreated(nodesMap.values.toMap, relsMap.toMap)
  }


  override def rels(types: Seq[String],
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
          getNodes(labels1, exact = false).flatMap{
            startNode =>
              relationStore
                .findOutRelations(startNode.id, typeId)
                .map(rel => (rel, nodeStore.getNodeLabelsById(rel.to)))
                .withFilter(_._2.toSeq.containsSlice(label2Id))
                .map(relEnd => (
                  if (includeProperty) relationStore.getRelationById(relEnd._1.id).map(mapRelation).get else mapRelation(relEnd._1),
                  if (includeStartNodes) Some(mapNode(startNode)) else None,
                  if (includeEndNodes) nodeStore.getNodeById(relEnd._1.to, relEnd._2.head).map(mapNode) else None
                ))
          }
        ).reduce(_++_)
      case (_, false, true)   => // [nodesWithLabel1]-[rels]-[nodesWithLabel2]
        getNodes(labels1, exact = false).flatMap{
          startNode =>
            relationStore
              .findOutRelations(startNode.id)
              .map(rel => (rel, nodeStore.getNodeLabelsById(rel.to)))
              .withFilter(_._2.toSeq.containsSlice(label2Id))
              .map(relEnd => (
                if (includeProperty) relationStore.getRelationById(relEnd._1.id).map(mapRelation).get else mapRelation(relEnd._1),
                if (includeStartNodes) Some(mapNode(startNode)) else None,
                if (includeEndNodes) nodeStore.getNodeById(relEnd._1.to, relEnd._2.head).map(mapNode) else None
              ))
        }
      case (_, true, false)   => // [nodesWithLabel1]-[relsWithTypes]-[nodes]
        typeIds.map(
          typeId =>
            getNodes(labels1, exact = false).flatMap{
              startNode =>
                relationStore
                  .findOutRelations(startNode.id, typeId)
                  .map(rel => (
                    if (includeProperty) relationStore.getRelationById(rel.id).map(mapRelation).get else mapRelation(rel),
                    if (includeStartNodes) Some(mapNode(startNode)) else None,
                    if (includeEndNodes) nodeAt(rel.to) else None
                  ))
            }
        ).reduce(_++_)
      case (true, false, false)  =>  // [nodesWithLabel1]-[allRel]-[allNodes]
        getNodes(labels1, exact = false).flatMap{
          startNode =>
            relationStore
              .findOutRelations(startNode.id)
              .map(rel => (
                if (includeProperty) relationStore.getRelationById(rel.id).map(mapRelation).get else mapRelation(rel),
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

  override def nodes(labels: Seq[String], exact: Boolean): Iterator[PandaNode] = getNodes(labels, exact).map(mapNode)

  def getNodes(labels: Seq[String], exact: Boolean): Iterator[StoredNode] = {
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
}
