package cn.pandadb.kernel.kv

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.{NameStore, Statistics}
import cn.pandadb.kernel.store._
import org.apache.logging.log4j.scala.Logging
import org.grapheco.lynx.{GraphModel, LynxId, LynxNode, LynxRelationship, LynxValue, NodeInput, RelationshipInput}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

class GraphFacadeWithPPD( nodeStore: NodeStoreSPI,
                          relationStore: RelationStoreSPI,
                          indexStore: IndexStoreAPI,
                          statistics: Statistics,
                          onClose: => Unit
                 ) extends Logging with GraphService with GraphModel{




  init()

  override def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = ???

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


  override def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]): this.type = {
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

  case class NodeId(value: Long) extends LynxId {

  }

  protected def mapNode(node: StoredNode): LynxNode = {
    new LynxNode {
      override val id: LynxId = NodeId(node.id)

      override def labels: Seq[String] = node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq

      override def property(name: String): Option[LynxValue] =
        node.asInstanceOf[StoredNodeWithProperty].properties.get(nodeStore.getPropertyKeyId(name)).map(LynxValue(_))
    }
  }

  override def rels(includeStartNodes: Boolean, includeEndNodes: Boolean): Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] = ???

  override def createElements[T](nodes: Array[(Option[String], NodeInput)], rels: Array[(Option[String], RelationshipInput)], onCreated: (Map[Option[String], LynxNode], Map[Option[String], LynxRelationship]) => T): T = ???

  override def nodes(): Iterator[LynxNode] = nodeStore.allNodes().map(mapNode)

  override def nodeAt(id: LynxId): Option[LynxNode] = ???
}
