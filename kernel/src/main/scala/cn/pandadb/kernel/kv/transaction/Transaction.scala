package cn.pandadb.kernel.kv.transaction

import cn.pandadb.kernel.kv.meta.{NodeIdGenerator, NodeLabelNameStore, PropertyNameStore, RelationIdGenerator, RelationTypeNameStore}
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
}

class NodeChange(oldLabels: Set[Int], oldProperties: Map[Int, Any]) extends PropertyChange(oldProperties) {
  val addedLabels = MutalbeSet[Int]()
  val removedLabels = MutalbeSet[Int]()
}

class RelationChange(relationType: Int, fromNode: Long, toNode: Long, oldProperties: Map[Int, Any])
  extends PropertyChange(oldProperties) {
}

class NodeInfo {
  val labels = ArrayBuffer[Int]()
  val properties = MutableMap[Int, Any]()
}

class RelationInfo(relationType: Int, fromNode: Long, toNode: Long) {
  val properties = MutableMap[Int, Any]()
}

class TransactionChanges {
  val changedNodes = MutableMap[Long, NodeChange]()
  val changedRelations = MutableMap[Long, RelationChange]()

  val addedNodes = MutableMap[Long, NodeInfo]()
  val deletedNodes = MutableMap[Long, NodeInfo]()

  val addedRelations = MutableMap[Long, RelationInfo]()
  val deletedRelations = MutableMap[Long, RelationInfo]()

  val createdNodeLabelTokens = MutableMap[String, Int]()
  val createdPropertyKeyTokens = MutableMap[String, Int]()
  val createdRelationTypeTokens = MutableMap[String, Int]()

  def nodeDoCreate(nodeId: Long): Unit = {
    this.addedNodes.put(nodeId, new NodeInfo())
  }
  def nodeDoDelete(nodeId: Long): Unit = ???
  def nodeDoAddLabel(nodeId: Long, labelId: Int): Unit = ???
  def nodeDoRemoveLabel(nodeId: Long, labelId: Int): Unit = ???
  def nodeDoSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit = ???
  def nodeDoRemoveProperty(nodeId: Long, propertyKeyId: Int): Unit = ???

  def relationDoCreate(relId: Long, fromNodeId: Long, toNodeId: Long, relationTypeId: Int): Unit = ???
  def relationDoSetProperty(relId: Long, propertyKeyId: Int, propertyValue: Any): Unit = ???
}


class StoreHolder(nodeIdGenerator: NodeIdGenerator,
                  relationIdGenerator: RelationIdGenerator,
//                  nodeLabelNameStore: NodeLabelNameStore,
//                  relationTypeNameStore: RelationTypeNameStore,
//                  propertyKeyNameStore: PropertyNameStore
              ) {
  // all store in this class should assure thread safety
  // all function is thread safe

  def newNodeId(): Long = nodeIdGenerator.nextId()

  def newRelationId(): Long = relationIdGenerator.nextId()
//
//  def getNodeLabelId(name: String): Int = nodeLabelNameStore.id(name)
//
//  def getNodeLabelName(id: Int): Option[String] = nodeLabelNameStore.key(id)

}

class Transaction(txId: Long,
                  storeHolder: StoreHolder
                 ) {
  val txChanges = new TransactionChanges()

  def assertOpen(): Unit = ???

}

class Writer(tx: Transaction, storeHolder: StoreHolder) {

  private def assureNodeExist(nodeId: Long): Unit = ???

  private def acquireNodeWriteLocked(nodeId: Long): Unit = ???

  private def acquireNodeReadLocked(nodeId: Long): Unit = ???

  private def assertTxOpen(): Unit = ???

  private def beforeChangeNode(nodeId: Long): Unit = {
    assertTxOpen()
    assureNodeExist(nodeId)
    acquireNodeWriteLocked(nodeId)
  }

  def nodeCreate(): Long = {
    assertTxOpen()
    val nodeId = storeHolder.newNodeId()
    tx.txChanges.nodeDoCreate(nodeId)
    nodeId
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
}