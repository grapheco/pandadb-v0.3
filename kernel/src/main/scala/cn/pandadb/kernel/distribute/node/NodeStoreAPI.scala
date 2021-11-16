package cn.pandadb.kernel.distribute.node

import cn.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import cn.pandadb.kernel.distribute.meta.{IdGenerator, NodeLabelNameStore, PropertyNameStore, TypeNameEnum}
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.serializer.BaseSerializer

import scala.collection.mutable.ArrayBuffer
/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-16 15:12
 */
class NodeStoreAPI(db: DistributedKVAPI, indexStore: PandaDistributedIndexStore) extends DistributedNodeStoreSPI{
  private val nodeLabelName = new NodeLabelNameStore(indexStore)
  private val propertyName = new PropertyNameStore(indexStore)
  private val idGenerator =new IdGenerator(db, TypeNameEnum.nodeName)

  val nodeStore = new NodeStore(db)
  val nodeLabelStore = new NodeLabelStore(db)
  val NONE_LABEL_ID: Int = 0

  override def newNodeId(): Long = idGenerator.nextId()

  override def hasLabel(nodeId: Long, label: Int): Boolean = nodeLabelStore.exist(nodeId, label)

  override def addNode(node: StoredNodeWithProperty): Unit ={
    if (node.labelIds.nonEmpty){
      nodeStore.set(node)
      nodeLabelStore.set(node.id, node.labelIds)
    }
  }

  override def addLabel(labelName: String): Int = nodeLabelName.getOrAddId(labelName)

  override def addPropertyKey(keyName: String): Int = propertyName.getOrAddId(keyName)

  override def deleteNode(nodeId: Long): Unit = {
    nodeLabelStore.getAll(nodeId).foreach(labelId => nodeStore.delete(nodeId, labelId))
    nodeLabelStore.delete(nodeId)
  }

  override def deleteNodes(nodeIDs: Iterator[Long]): Unit = {
    val nodeKeys = ArrayBuffer[Array[Byte]]()

    nodeIDs.grouped(1000).foreach(groupIds =>{
      groupIds.foreach(nId => {
        nodeLabelStore.getAll(nId).foreach(labelId => nodeKeys.append(DistributedKeyConverter.toNodeKey(labelId, nId)))
        // TODO: check correct
        nodeLabelStore.deleteRange(DistributedKeyConverter.toNodeLabelKey(nId, 0),
          DistributedKeyConverter.toNodeLabelKey(nId, -1))
      })

      nodeStore.batchDelete(nodeKeys.toSeq)
      nodeKeys.clear()
    })
  }

  override def deleteNodesByLabel(labelId: Int): Unit = {
    nodeStore.getNodeIdsByLabel(labelId).grouped(1000).foreach(groupNodeIds => {
      groupNodeIds.foreach(nId => {
        nodeLabelStore.deleteRange(DistributedKeyConverter.toNodeLabelKey(nId, 0),
          DistributedKeyConverter.toNodeLabelKey(nId, -1))
      })
    })
    nodeStore.deleteByLabel(labelId)
  }

  override def getLabelName(labelId: Int): Option[String] = nodeLabelName.key(labelId)

  override def getLabelId(labelName: String): Option[Int] = nodeLabelName.id(labelName)

  override def getLabelIds(labelNames: Set[String]): Set[Int] = nodeLabelName.ids(labelNames)

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] = propertyName.id(keyName)

  override def getNodeById(nodeId: Long): Option[StoredNodeWithProperty] = {
    nodeLabelStore.get(nodeId).map(labelId=>nodeStore.get(nodeId, labelId).get)
  }

  override def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty] = nodeStore.get(nodeId, label)

  override def getNodeById(nodeId: Long, label: Option[Int]): Option[StoredNodeWithProperty] =
    label.map(getNodeById(nodeId, _)).getOrElse(getNodeById(nodeId))

  override def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty] = nodeStore.getNodesByLabel(labelId)

  override def getNodeIdsByLabel(labelId: Int): Iterator[Long] = nodeStore.getNodeIdsByLabel(labelId)

  override def getNodeLabelsById(nodeId: Long): Array[Int] = nodeLabelStore.getAll(nodeId)

  override def allLabels(): Array[String] = ???

  override def allLabelIds(): Array[Int] = ???

  override def allPropertyKeys(): Array[String] = ???

  override def allPropertyKeyIds(): Array[Int] = ???

  override def allNodes(): Iterator[StoredNodeWithProperty] = ???

  override def nodesCount: Long = ???

  override def close(): Unit = ???

  override def nodeAddLabel(nodeId: Long, labelId: Int): Unit = {

  }

  override def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit = ???

  override def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit = ???

  override def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any = ???
}

trait DistributedNodeStoreSPI {
  def newNodeId(): Long;

  def hasLabel(nodeId: Long, label: Int): Boolean;

  def addNode(node: StoredNodeWithProperty): Unit

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
