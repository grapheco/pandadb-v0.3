//package cn.pandadb.kernel.kv
//
//import cn.pandadb.kernel.kv.index.IndexStoreAPI
//import cn.pandadb.kernel.kv.meta.Statistics
//import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty, StoredRelationWithProperty}
//import org.opencypher.lynx.ir.{IRContextualNodeRef, IRNode, IRNodeRef, IRRelation, IRStoredNodeRef, PropertyGraphWriter}
//import org.opencypher.okapi.api.value.CypherValue
//import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue, Node, NoopCypherValueConverter, Relationship}
//
//class PandaPropertyGraphWriterImpl(nodeStore: NodeStoreSPI,
//                                   relationStore: RelationStoreSPI,
//                                   indexStore: IndexStoreAPI,
//                                   statistics: Statistics) extends PropertyGraphWriter[Long]{
//
//  def addNode(labels: Seq[String], props: Seq[(String, CypherValue)]): Long = {
//    val id = nodeStore.newNodeId()
//    val labelsId = labels.map(nodeStore.getLabelId).toArray
//    val propsMap  = props.toMap
//      .map(kv => (nodeStore.getPropertyKeyId(kv._1), kv._2.getValue))
//      .filter(_._2.isDefined).mapValues(_.get)
//    nodeStore.addNode(new StoredNodeWithProperty(id, labelsId, propsMap))
//    statistics.increaseNodeCount(1) // TODO batch
//    labelsId.foreach(statistics.increaseNodeLabelCount(_, 1))
//    id
//  }
//
//  def addRel(types: Seq[String], props: Seq[(String, CypherValue)], startId: Long, endId: Long): Long = {
//    val id = relationStore.newRelationId()
//    val typesId = types.map(relationStore.getRelationTypeId).toArray //TODO only one type supported
//    val propsMap  = props.toMap
//      .map(kv => (relationStore.getPropertyKeyId(kv._1), kv._2.getValue))
//      .filter(_._2.isDefined).mapValues(_.get)
//    relationStore.addRelation(new StoredRelationWithProperty(id, startId, endId, typesId(0), propsMap))
//    statistics.increaseRelationCount(1) // TODO batch
//    statistics.increaseRelationTypeCount(typesId(0), 1) // TODO only one type supported, batch
//    id
//  }
//
//  override def createElements(nodes: Array[IRNode], rels: Array[IRRelation[Long]]): Unit = {
//    nodes.foreach{
//      node => addNode(node.labels, node.props)
//    }
//    rels.foreach{
//      rel =>
//
//        addRel(rel.types, rel.props, mergeNodeId(rel.startNodeRef), mergeNodeId(rel.endNodeRef))
//    }
//
//    def mergeNodeId(ref: IRNodeRef[Long]):Long = {
//      ref match {
//        case IRStoredNodeRef(id) => id
//        case IRContextualNodeRef(node) => addNode(node.labels, node.props)
//      }
//    }
//  }
//}
