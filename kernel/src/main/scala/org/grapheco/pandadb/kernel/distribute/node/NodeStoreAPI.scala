package org.grapheco.pandadb.kernel.distribute.node

import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.distribute.meta.{IdGenerator, NodeLabelNameStore, PropertyNameStore, TypeNameEnum}
import org.grapheco.pandadb.kernel.store.StoredNodeWithProperty
import org.grapheco.pandadb.kernel.util.serializer.BaseSerializer

import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-16 15:12
 */
class NodeStoreAPI(db: DistributedKVAPI, propertyNameStore: PropertyNameStore) extends DistributedNodeStoreSPI {
  private val nodeLabelName = new NodeLabelNameStore(db, propertyNameStore.udpClientManager)
  private val idGenerator = new IdGenerator(db, TypeNameEnum.nodeName)

  val nodeStore = new NodeStore(db)
  val nodeLabelStore = new NodeLabelStore(db)
  val NONE_LABEL_ID: Int = 0

  override def refreshMeta(): Unit ={
    nodeLabelName.refreshNameStore()
    propertyNameStore.refreshNameStore()
    idGenerator.refreshId()
  }

  override def newNodeId(): Long = idGenerator.nextId()

  override def cleanData(): Unit = idGenerator.resetId()

  override def hasLabel(nodeId: Long, label: Int): Boolean = nodeLabelStore.exist(nodeId, label)

  override def addNode(node: StoredNodeWithProperty): Unit = {
    if (node.labelIds.nonEmpty) {
      nodeStore.set(node)
      nodeLabelStore.set(node.id, node.labelIds)
    }
    else {
      nodeStore.set(NONE_LABEL_ID, node)
      nodeLabelStore.set(node.id, NONE_LABEL_ID)
    }
  }

  override def addNodes(nodes: Seq[StoredNodeWithProperty]): Unit = {
    nodeStore.batchPut(nodes)
    nodeLabelStore.batchPut(nodes)
  }

  override def addLabel(labelName: String): Int = nodeLabelName.getOrAddId(labelName)

  override def addPropertyKey(keyName: String): Int = propertyNameStore.getOrAddId(keyName)

  override def deleteNode(nodeId: Long): Unit = {
    nodeLabelStore.getAll(nodeId).foreach(labelId => {
      nodeStore.delete(nodeId, labelId)
    })
    nodeLabelStore.deletePrefix(nodeId)
  }

  override def deleteNodes(nodeIDs: Iterator[Long]): Unit = {
    val batchDelete = ArrayBuffer[Array[Byte]]()
    nodeIDs.grouped(1000).foreach(
      groupIds => {
        groupIds.foreach(nId => {
          nodeLabelStore.getAll(nId).foreach(labelId => {
            batchDelete.append(DistributedKeyConverter.toNodeKey(labelId, nId))
          })

          nodeStore.batchDelete(batchDelete.toSeq)
          batchDelete.clear()

          nodeLabelStore.deletePrefix(nId)
        })
      }
    )
  }

  override def deleteNodesByLabel(labelId: Int): Unit = {
    nodeStore.getNodeIdsByLabel(labelId).foreach {
      nodeId => {
        nodeLabelStore.getAll(nodeId).foreach(labelId => nodeStore.delete(nodeId, labelId))
        nodeLabelStore.deletePrefix(nodeId)
      }
    }
    nodeStore.deleteByLabel(labelId)
  }

  override def getLabelName(labelId: Int): Option[String] = nodeLabelName.key(labelId)

  override def getLabelId(labelName: String): Option[Int] = nodeLabelName.id(labelName)

  override def getLabelIds(labelNames: Set[String]): Set[Int] = nodeLabelName.ids(labelNames)

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyNameStore.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] = propertyNameStore.id(keyName)

  override def getNodeById(nodeId: Long): Option[StoredNodeWithProperty] = {
    nodeLabelStore.get(nodeId).map(labelId => nodeStore.get(nodeId, labelId).get)
  }

  override def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty] = nodeStore.get(nodeId, label)

  override def getNodeById(nodeId: Long, label: Option[Int]): Option[StoredNodeWithProperty] =
    label.map(getNodeById(nodeId, _)).getOrElse(getNodeById(nodeId))

  override def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty] = nodeStore.getNodesByLabel(labelId)

  override def getNodesByIds(labelId: Int, ids: Seq[Long]): Iterator[StoredNodeWithProperty] = {
    nodeStore.getNodesByIds(labelId, ids)
  }

  override def getNodeIdsByLabel(labelId: Int): Iterator[Long] = nodeStore.getNodeIdsByLabel(labelId)

  override def getNodeLabelsById(nodeId: Long): Array[Int] = nodeLabelStore.getAll(nodeId)

  override def allLabels(): Array[String] = nodeLabelName.mapString2Int.keys.toArray

  override def allLabelIds(): Array[Int] = nodeLabelName.mapInt2String.keys.toArray

  override def allPropertyKeys(): Array[String] = propertyNameStore.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyNameStore.mapInt2String.keys.toArray

  override def allNodes(): Iterator[StoredNodeWithProperty] = nodeStore.all()

  override def nodesCount: Long = nodeLabelStore.getNodesCount

  override def close(): Unit = {

  }

  override def nodeAddLabel(nodeId: Long, labelId: Int): Unit = {
    val node = getNodeById(nodeId).get
    if (!node.labelIds.contains(labelId)) {
      val labels = node.labelIds ++ Array(labelId)
      nodeLabelStore.set(nodeId, labels)
      nodeStore.set(new StoredNodeWithProperty(nodeId, labels, node.properties))

      if (node.labelIds.isEmpty) {
        nodeLabelStore.delete(nodeId, NONE_LABEL_ID)
        nodeStore.delete(nodeId, NONE_LABEL_ID)
      }
    }
  }

  override def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit = {
    val node = nodeStore.get(nodeId, labelId).get
    if (node.labelIds.contains(labelId)) {
      val labels = node.labelIds.filter(_ != labelId)
      val newNode = new StoredNodeWithProperty(nodeId, labels, node.properties)
      if (labels.length == 0) {
        nodeLabelStore.set(nodeId, NONE_LABEL_ID)
      }
      nodeStore.delete(nodeId, labelId)
      nodeLabelStore.delete(nodeId, labelId)
      nodeStore.set(newNode)
    }
  }

  override def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    val node = getNodeById(nodeId).get
    nodeStore.set(new StoredNodeWithProperty(node.id, node.labelIds, node.properties ++ Map(propertyKeyId -> propertyValue)))
  }

  override def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any = {
    val node = getNodeById(nodeId).get
    nodeStore.set(new StoredNodeWithProperty(node.id, node.labelIds, node.properties - propertyKeyId))
  }
}

trait DistributedNodeStoreSPI {
  def refreshMeta(): Unit

  def newNodeId(): Long;

  def cleanData(): Unit

  def hasLabel(nodeId: Long, label: Int): Boolean;

  def addNode(node: StoredNodeWithProperty): Unit

  def addNodes(nodes: Seq[StoredNodeWithProperty]): Unit

  def addLabel(labelName: String): Int;

  def addPropertyKey(keyName: String): Int;

  def nodeAddLabel(nodeId: Long, labelId: Int): Unit;

  def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit;

  def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any;

  def deleteNode(nodeId: Long): Unit;

  def deleteNodes(nodeIDs: Iterator[Long]): Unit;

  def deleteNodesByLabel(labelId: Int): Unit

  def getLabelName(labelId: Int): Option[String];

  def getLabelId(labelName: String): Option[Int];

  def getLabelIds(labelNames: Set[String]): Set[Int]

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def getNodeById(nodeId: Long): Option[StoredNodeWithProperty]

  def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty]

  def getNodeById(nodeId: Long, label: Option[Int]): Option[StoredNodeWithProperty]

  def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty];

  def getNodesByIds(labelId: Int, ids: Seq[Long]): Iterator[StoredNodeWithProperty]

  def getNodeIdsByLabel(labelId: Int): Iterator[Long];

  def getNodeLabelsById(nodeId: Long): Array[Int];

  def serializeLabelIdsToBytes(labelIds: Array[Int]): Array[Byte] = {
    BaseSerializer.array2Bytes(labelIds)
  }

  def deserializeBytesToLabelIds(bytes: Array[Byte]): Array[Int] = {
    BaseSerializer.bytes2Array(bytes).asInstanceOf[Array[Int]]
  }

  def serializePropertiesToBytes(properties: Map[Int, Any]): Array[Byte] = {
    BaseSerializer.map2Bytes(properties)
  }

  def deserializeBytesToProperties(bytes: Array[Byte]): Map[Int, Any] = {
    BaseSerializer.bytes2Map(bytes)
  }

  def allLabels(): Array[String];

  def allLabelIds(): Array[Int];

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def allNodes(): Iterator[StoredNodeWithProperty]

  def nodesCount: Long

  def close(): Unit
}
