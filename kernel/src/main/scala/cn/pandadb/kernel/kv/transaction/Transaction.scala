package cn.pandadb.kernel.kv.transaction

import cn.pandadb.kernel.kv.meta.{NodeIdGenerator, NodeLabelNameStore, PropertyNameStore, RelationIdGenerator, RelationTypeNameStore}
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{StoredNodeWithProperty, StoredRelationWithProperty}
import org.opencypher.v9_0.expressions.LabelToken

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{Map => MutableMap, Set => MutalbeSet}
import scala.collection.immutable.Set

class PropertyChange(oldProperties: Map[Int, Any]) {
  val newProperties = MutableMap[Int, Any]()
  oldProperties.foreach(kv => newProperties.put(kv._1, kv._2))

  val added = MutalbeSet[Int]()
  val removed = MutalbeSet[Int]()
  val updated = MutalbeSet[Int]()

  def setProperty(keyId: Int, value: Any): Unit = {
    if (oldProperties.contains(keyId)) {
      if (!oldProperties.get(keyId).get.equals(value)) {
        updated.add(keyId)
        newProperties.put(keyId, value)
      }
    }
    else {
      added.add(keyId)
      newProperties.put(keyId, value)
    }
  }

  def removeProperty(keyId: Int): Unit = {
    if (oldProperties.contains(keyId)) {
      removed.add(keyId)
      updated.remove(keyId)
      newProperties.remove(keyId)
    }
    else {
      added.remove(keyId)
      newProperties.remove(keyId)
    }
  }
}

class NodeChange(oldLabels: Set[Int], oldProperties: Map[Int, Any]) extends PropertyChange(oldProperties) {
  val addedLabels = MutalbeSet[Int]()
  val removedLabels = MutalbeSet[Int]()

  def addLabel(labelId: Int): Boolean = {
    if (oldLabels.contains(labelId)) false
    else {
      addedLabels.add(labelId)
      true
    }
  }

  def removeLabel(labelId: Int): Boolean = {
    if (!oldLabels.contains(labelId)) {
      false
    }
    else {
      removedLabels.add(labelId)
      true
    }
  }
}

class RelationChange(relationType: Int, fromNode: Long, toNode: Long, oldProperties: Map[Int, Any])
  extends PropertyChange(oldProperties) {
}

class NodeInfo {
  val labels = MutalbeSet[Int]()
  val properties = MutableMap[Int, Any]()
}
object NodeInfo {
  def fromStoredNode(node: StoredNodeWithProperty): NodeInfo = {
    val nodeInfo = new NodeInfo()
    node.labelIds.foreach(label => nodeInfo.labels.add(label))
    node.properties.foreach(kv => nodeInfo.properties.put(kv._1, kv._2))
    nodeInfo
  }
}

class RelationInfo(relationType: Int, fromNode: Long, toNode: Long) {
  val properties = MutableMap[Int, Any]()
}
object RelationInfo {
  def fromStoredRelation(relation: StoredRelationWithProperty): RelationInfo = {
    val relationInfo = new RelationInfo(relation.typeId, relation.from, relation.to)
    relation.properties.foreach(kv => relationInfo.properties.put(kv._1, kv._2))
    relationInfo
  }
}


class TransactionChanges(storeHolder: StoreHolder) {
  val changedNodes = MutableMap[Long, NodeChange]()
  val changedRelations = MutableMap[Long, RelationChange]()

  val addedNodes = MutableMap[Long, NodeInfo]()
  val deletedNodesFromStore = MutableMap[Long, NodeInfo]()

  val addedRelations = MutableMap[Long, RelationInfo]()
  val deletedRelationsFromStore = MutableMap[Long, RelationInfo]()

  val createdNodeLabelTokens = MutableMap[String, Int]()
  val createdPropertyKeyTokens = MutableMap[String, Int]()
  val createdRelationTypeTokens = MutableMap[String, Int]()

  def hasNodeDataChanges(): Boolean = {
    changedNodes.size > 0 || addedNodes.size > 0 || deletedNodes.size > 0
  }

  def hasRelationDataChanges(): Boolean = {
    changedRelations.size > 0 || addedRelations.size > 0 || deletedRelations.size > 0
  }

  def hasDataChanges(): Boolean = {
     hasDataChanges() || hasRelationDataChanges()
  }

  private def getNodeFromStore(nodeId: Long): Option[StoredNodeWithProperty] = {
    storeHolder.getNodeFromStore(nodeId)
  }

  private def getRelationFromStore(relationId: Long): Option[StoredRelationWithProperty] = {
    storeHolder.getRelationFromStore(relationId)
  }

  def nodeDoCreate(nodeId: Long): Unit = {
    this.addedNodes.put(nodeId, new NodeInfo())
  }
  def nodeDoDelete(nodeId: Long): Unit = {
    if(this.addedNodes.contains(nodeId)) {
      addedNodes.remove(nodeId)
    }
    else {
      val node = this.getNodeFromStore(nodeId)
      if (node.isEmpty) { throw new Exception(s"node(${nodeId}) is not exist")}
      else {
        deletedNodesFromStore.put(nodeId, NodeInfo.fromStoredNode(node.get))
        changedNodes.remove(nodeId)
      }
    }
  }

  def nodeDoAddLabel(nodeId: Long, labelId: Int): Unit = {
    if (this.addedNodes.contains(nodeId)) {
      this.addedNodes(nodeId).labels.add(labelId)
      return
    }
    if (this.deletedNodesFromStore.contains(nodeId)){
      throw new Exception(s"add label to not exist node(${nodeId})")
    }

    var nodeChange: NodeChange = null
    if (changedNodes.contains(nodeId)) {
      nodeChange = changedNodes.get(nodeId).get
    }
    else {
      val storedNode = getNodeFromStore(nodeId)
      if (storedNode.isEmpty) {
        throw new Exception(s"add label to not exist node(${nodeId})")
      }
      nodeChange = new NodeChange(storedNode.get.labelIds.toSet, storedNode.get.properties)
      changedNodes.put(nodeId, nodeChange)
    }
    nodeChange.addLabel(labelId)
  }

  def nodeDoRemoveLabel(nodeId: Long, labelId: Int): Unit = {
    if (this.addedNodes.contains(nodeId)) {
      this.addedNodes(nodeId).labels.remove(labelId)
      return
    }
    if (this.deletedNodesFromStore.contains(nodeId)){
      throw new Exception(s"add label to not exist node(${nodeId})")
    }

    var nodeChange: NodeChange = null
    if (changedNodes.contains(nodeId)) {
      nodeChange = changedNodes.get(nodeId).get
    }
    else {
      val storedNode = getNodeFromStore(nodeId)
      if (storedNode.isEmpty) {
        throw new Exception(s"add label to not exist node(${nodeId})")
      }
      nodeChange = new NodeChange(storedNode.get.labelIds.toSet, storedNode.get.properties)
      changedNodes.put(nodeId, nodeChange)
    }
    nodeChange.removeLabel(labelId)
  }

  def nodeDoSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    if (this.addedNodes.contains(nodeId)) {
      this.addedNodes(nodeId).properties.put(propertyKeyId, propertyValue)
      return
    }
    if (this.deletedNodesFromStore.contains(nodeId)){
      throw new Exception(s"set property to not exist node(${nodeId})")
    }

    if (!changedNodes.contains(nodeId)) {
      val storedNode = getNodeFromStore(nodeId)
      if (storedNode.isEmpty) {
        throw new Exception(s"set property to not exist node(${nodeId})")
      }
      changedNodes.put(nodeId, new NodeChange(storedNode.get.labelIds.toSet, storedNode.get.properties))
    }
    changedNodes.get(nodeId).get.setProperty(propertyKeyId, propertyValue)
  }

  def nodeDoRemoveProperty(nodeId: Long, propertyKeyId: Int): Unit = {
    if (this.addedNodes.contains(nodeId)) {
      this.addedNodes(nodeId).properties.remove(propertyKeyId)
      return
    }
    if (this.deletedNodesFromStore.contains(nodeId)){
      throw new Exception(s"remove property from not exist node(${nodeId})")
    }

    if (!changedNodes.contains(nodeId)) {
      val storedNode = getNodeFromStore(nodeId)
      if (storedNode.isEmpty) {
        throw new Exception(s"remove property from not exist node(${nodeId})")
      }
      changedNodes.put(nodeId, new NodeChange(storedNode.get.labelIds.toSet, storedNode.get.properties))
    }
    changedNodes.get(nodeId).get.removeProperty(propertyKeyId)
  }

  def relationDoCreate(relationId: Long, fromNodeId: Long, toNodeId: Long, relationTypeId: Int): Unit = {
    val relationInfo = new RelationInfo(relationTypeId, fromNodeId, toNodeId)
    this.addedRelations.put(relationId, relationInfo)
  }

  def relationDoDelete(relationId: Long): Unit = {
    if(this.addedRelations.contains(relationId)) {
      this.addedRelations.remove(relationId)
    }
    else {
      val relation = this.getRelationFromStore(relationId)
      if (relation.isEmpty) { throw new Exception(s"relation(${relationId}) is not exist")}
      else {
        deletedRelationsFromStore.put(relationId, RelationInfo.fromStoredRelation(relation.get))
      }
    }
  }

  def relationDoSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    if (this.addedRelations.contains(relationId)) {
      this.addedRelations(relationId).properties.put(propertyKeyId, propertyValue)
      return
    }
    if (this.deletedRelationsFromStore.contains(relationId)){
      throw new Exception(s"set property to not exist Relation(${relationId})")
    }

    if (!changedRelations.contains(relationId)) {
      val storedRelation = getRelationFromStore(relationId)
      if (storedRelation.isEmpty) {
        throw new Exception(s"set property to not exist Relation(${relationId})")
      }
      val r = storedRelation.get
      changedRelations.put(relationId, new RelationChange(r.typeId, r.from, r.to, r.properties))
    }
    changedRelations.get(relationId).get.setProperty(propertyKeyId, propertyValue)
  }

  def relationDoRemoveProperty(relationId: Long, propertyKeyId: Int): Unit = {
    if (this.addedRelations.contains(relationId)) {
      this.addedRelations(relationId).properties.remove(propertyKeyId)
      return
    }
    if (this.deletedRelationsFromStore.contains(relationId)){
      throw new Exception(s"set property to not exist Relation(${relationId})")
    }

    if (!changedRelations.contains(relationId)) {
      val storedRelation = getRelationFromStore(relationId)
      if (storedRelation.isEmpty) {
        throw new Exception(s"set property to not exist Relation(${relationId})")
      }
      val r = storedRelation.get
      changedRelations.put(relationId, new RelationChange(r.typeId, r.from, r.to, r.properties))
    }
    changedRelations.get(relationId).get.removeProperty(propertyKeyId)
  }
}


class StoreHolder(nodeIdGenerator: NodeIdGenerator,
                  relationIdGenerator: RelationIdGenerator,
//                  nodeLabelNameStore: NodeLabelNameStore,
//                  relationTypeNameStore: RelationTypeNameStore,
//                  propertyKeyNameStore: PropertyNameStore
                 nodeStore: NodeStoreAPI,
                  relationStore: RelationStoreAPI
              ) {
  // all store in this class should assure thread safety
  // all function is thread safe

  def newNodeId(): Long = nodeIdGenerator.nextId()

  def newRelationId(): Long = relationIdGenerator.nextId()
//
//  def getNodeLabelId(name: String): Int = nodeLabelNameStore.id(name)
//
//  def getNodeLabelName(id: Int): Option[String] = nodeLabelNameStore.key(id)

  def getNodeFromStore(nodeId: Long): Option[StoredNodeWithProperty] = {
    nodeStore.getNodeById(nodeId)
  }

  def getRelationFromStore(relationId: Long): Option[StoredRelationWithProperty] = {
    relationStore.getRelationById(relationId)
  }

}

class Transaction(txId: Long,
                  storeHolder: StoreHolder
                 ) {
  val txChanges = new TransactionChanges(storeHolder)

  def assertOpen(): Unit = ???

}

class Writer(tx: Transaction, storeHolder: StoreHolder) {

  // include in this tx
  private def assureNodeExist(nodeId: Long): Unit = ???

  private def acquireNodeWriteLocked(nodeId: Long): Unit = ???

  private def acquireNodeReadLocked(nodeId: Long): Unit = ???

  private def assureRelationExist(relationId: Long): Unit = ???

  private def acquireRelationWriteLocked(RelationId: Long): Unit = ???

  private def assertTxOpen(): Unit = ???

  private def beforeChangeNode(nodeId: Long): Unit = {
    assertTxOpen()
    assureNodeExist(nodeId)
    acquireNodeWriteLocked(nodeId)
  }

  private def beforeChangeRelation(relationId: Long): Unit = {
    assertTxOpen()
    assureRelationExist(relationId)
    acquireRelationWriteLocked(relationId)
  }

  def nodeCreate(): Long = {
    assertTxOpen()
    val nodeId = storeHolder.newNodeId()
    tx.txChanges.nodeDoCreate(nodeId)
    nodeId
  }

  def nodeDelete(nodeId: Long): Unit = {
    beforeChangeNode(nodeId)
    tx.txChanges.nodeDoDelete(nodeId)
  }

  def nodeAddLabel(nodeId: Long, labelId: Int): Unit = {
    beforeChangeNode(nodeId)
    tx.txChanges.nodeDoAddLabel(nodeId, labelId)
  }

  def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit = {
    beforeChangeNode(nodeId)
    tx.txChanges.nodeDoRemoveLabel(nodeId, labelId)
  }

  def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    beforeChangeNode(nodeId)
    tx.txChanges.nodeDoSetProperty(nodeId, propertyKeyId, propertyValue)
  }

  def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Unit = {
    beforeChangeNode(nodeId)
    tx.txChanges.nodeDoRemoveProperty(nodeId: Long, propertyKeyId: Int)
  }

  def relationCreate(fromNodeId: Long, toNodeId: Long, relationTypeId: Int): Long = {
    assertTxOpen()
    acquireNodeReadLocked(fromNodeId)
    acquireNodeReadLocked(toNodeId)

    val relId = storeHolder.newRelationId()
    tx.txChanges.relationDoCreate(relId, fromNodeId, toNodeId, relationTypeId)
    relId
  }

  def relationDelete(relationId: Long): Unit = {
    beforeChangeRelation(relationId)
    tx.txChanges.relationDoDelete(relationId)
  }

  def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    beforeChangeRelation(relationId)
    tx.txChanges.relationDoSetProperty(relationId, propertyKeyId, propertyValue)
  }

  def relationRemoveProperty(relationId: Long, propertyKeyId: Int): Unit = {
    beforeChangeRelation(relationId)
    tx.txChanges.relationDoRemoveProperty(relationId: Long, propertyKeyId: Int)
  }

}