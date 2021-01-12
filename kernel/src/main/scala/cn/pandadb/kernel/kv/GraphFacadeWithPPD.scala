package cn.pandadb.kernel.kv

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.{NameStore, Statistics}
import cn.pandadb.kernel.store._
import org.apache.logging.log4j.scala.Logging
import org.grapheco.lynx.{ContextualNodeInputRef, CypherRunner, GraphModel, LynxId, LynxNode, LynxRelationship, LynxResult, LynxValue, NodeInput, NodeInputRef, RelationshipInput, StoredNodeInputRef}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

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
          if (propsIndex._1.length<=1){
            indexStore.insertIndexRecord(propsIndex._2, props.getOrElse(propsIndex._1(0), null), nodeId)
          }else {
            // TODO combined index
          }
          statistics.increaseIndexPropertyCount(propsIndex._2, 1)
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
              if (propsIndex._1.length<=1){
                indexStore.deleteIndexRecord(propsIndex._2, node.properties.getOrElse(propsIndex._1(0), null), node.id)
              }else {
                // TODO combined index
              }
              statistics.decreaseIndexPropertyCount(propsIndex._2, 1)
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
      id =>
        statistics.setIndexPropertyCount(id,
          indexStore.findByPrefix(ByteUtils.intToBytes(id)).length)
        println(s"index ${id} count: ${statistics.getIndexPropertyCount(id)}")
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

  protected def mapNode(node: StoredNode): LynxNode = {
    new LynxNode {
      override val id: LynxId = NodeId(node.id)

      override def labels: Seq[String] = node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq

      override def property(name: String): Option[LynxValue] =
        node.asInstanceOf[StoredNodeWithProperty].properties.get(nodeStore.getPropertyKeyId(name)).map(LynxValue(_))
    }
  }

  protected def mapRelation(rel: StoredRelation): LynxRelationship = {
   new LynxRelationship {
     override val id: LynxId = RelationId(rel.id)
     override val startNodeId: LynxId = NodeId(rel.from)
     override val endNodeId: LynxId = NodeId(rel.to)

     override def relationType: Option[String] = relationStore.getRelationTypeName(rel.typeId)

     override def property(name: String): Option[LynxValue] = rel.asInstanceOf[StoredRelationWithProperty].properties.get(relationStore.getPropertyKeyId(name)).map(LynxValue(_))
   }
  }

  override def rels(includeStartNodes: Boolean, includeEndNodes: Boolean): Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] = {
    relationStore.allRelations().map(mapRelation).map(rel =>
      (rel,
        if (includeStartNodes) {
          nodeAt(rel.startNodeId)
        }
        else {
          None
        },
        if (includeEndNodes) {
          nodeAt(rel.endNodeId)
        }
        else {
          None
        })
    )
  }
  case class NodeId(value: Long) extends LynxId {}
  case class RelationId(value: Long) extends LynxId {}
  case class TestNode(longId: Long, labels: Seq[String], props: (String, LynxValue)*) extends LynxNode {
    lazy val properties = props.toMap
    override val id: LynxId = NodeId(longId)

    override def property(name: String): Option[LynxValue] = properties.get(name)
  }

  case class TestRelationship(id0: Long, startId: Long, endId: Long, relationType: Option[String], props: (String, LynxValue)*) extends LynxRelationship {
    lazy val properties = props.toMap
    override val id: LynxId = RelationId(id0)
    override val startNodeId: LynxId = NodeId(startId)
    override val endNodeId: LynxId = NodeId(endId)

    override def property(name: String): Option[LynxValue] = properties.get(name)
  }

  override def createElements[T](nodes: Array[(Option[String], NodeInput)], rels: Array[(Option[String], RelationshipInput)], onCreated: (Map[Option[String], LynxNode], Map[Option[String], LynxRelationship]) => T): T = {
    val nodesMap: Map[NodeInput, (Option[String], TestNode)] = nodes.map(x => {
      val (varname, input) = x
      val id = nodeStore.newNodeId()
      input -> (varname, TestNode(id, input.labels, input.props: _*))
    }).toMap

    def nodeId(ref: NodeInputRef): Long = {
      ref match {
        case StoredNodeInputRef(id) => id.value.asInstanceOf[Long]
        case ContextualNodeInputRef(node) => nodesMap(node)._2.longId
      }
    }

    val relsMap: Array[(Option[String], TestRelationship)] = rels.map(x => {
      val (varname, input) = x
      varname -> TestRelationship(relationStore.newRelationId(), nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption)
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



  override def nodes(): Iterator[LynxNode] = nodeStore.allNodes().map(mapNode)

  override def nodeAt(id: LynxId): Option[LynxNode] = Some(mapNode(nodeStore.getNodeById(id.value.asInstanceOf[Long]).get))
}
