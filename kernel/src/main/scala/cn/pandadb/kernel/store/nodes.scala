package cn.pandadb.kernel.store

import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.opencypher.v9_0.util.LabelId

//
//trait ReadOnlyNode {
//
//  def getId(): Long;
//
//  def getProperty(key: String): Any;
//
//  def getAllProperties(): Map[String, Any];
//
//  def getLabels(): Array[String];
//}
//
//trait WritableNode extends ReadOnlyNode{
//  def setProperty(key:String, value: Any): Unit = ???;
//
//  def removeProperty(key:String): Any = ???;
//
//  def addLabel(label: String): Unit = ???;
//
//  def removeLabel(label: String): Unit = ???;
//}
//
//class LazyWritableNode(id: Long, nodeStoreSpi: NodeStoreSPI) extends LazyNode(id, nodeStoreSpi) with WritableNode {
//  override def setProperty(key:String, value: Any): Unit = {
//    nodeStoreSpi.nodeSetProperty(id, nodeStoreSpi.getPropertyKeyId(key), value)
//  }
//
//  override def removeProperty(key:String): Any = {
//    nodeStoreSpi.nodeRemoveProperty(id, nodeStoreSpi.getPropertyKeyId(key))
//  }
//
//  override def addLabel(label: String): Unit = {
//    nodeStoreSpi.nodeAddLabel(id, nodeStoreSpi.getLabelId(label))
//  }
//
//  override def removeLabel(label: String): Unit = {
//    nodeStoreSpi.nodeRemoveLabel(id, nodeStoreSpi.getLabelId(label))
//  }
//
//}

//class SerializedNode(id:Long,
//                     override val labelIdsBytes: Array[Byte],
//                     override val propertiesBytes: Array[Byte],
//                     nodeStoreSpi: NodeStoreSPI)
//  extends LazyNode(id, nodeStoreSpi) {
//}

//class LazyNode(id: Long, nodeStoreSpi: NodeStoreSPI) extends ReadOnlyNode {
//
//  lazy val labelIdsBytes: Array[Byte] = nodeStoreSpi.getNodeLabelIdsBytes(id)
//  lazy val propertiesBytes: Array[Byte] = nodeStoreSpi.getNodePropertiesBytes(id)
//
//  lazy val labelIds: Array[Int] = nodeStoreSpi.deserializeBytesToLabelIds(labelIdsBytes)
//  lazy val propertyMap: Map[Int, Any] = nodeStoreSpi.deserializeBytesToProperties(propertiesBytes)
//
//  def getLabelIds(): Array[Int] = {
//    labelIds
//  }
//
//  def getPropertyByKeyId(keyId: Int): Option[Any] = {
//    propertyMap.get(keyId)
//  }
//
//  override def getId(): Long = id
//
//  override def getLabels(): Array[String] = {
//    labelIds.map(id => nodeStoreSpi.getLabelName(id))
//  }
//
//  override def getProperty(key: String): Any = {
//    propertyMap.get(nodeStoreSpi.getPropertyKeyId(key)).get
//  }
//
//  override def getAllProperties(): Map[String, Any] = {
//    propertyMap.map(kv => (nodeStoreSpi.getPropertyKeyName(kv._1), kv._2))
//  }
//
//}

//class NodeWithProperty(id:Long,
//                     override val propertiesBytes: Array[Byte],
//                     nodeStoreSpi: NodeStoreSPI)
//  extends LazyNode(id, nodeStoreSpi) {
//}

//class NodeWithLabels(id:Long,
//                       override val labelIdsBytes: Array[Byte],
//                       nodeStoreSpi: NodeStoreSPI)
//  extends LazyNode(id, nodeStoreSpi) {
//}

trait StoredValue{

}

case class StoredValueNull() extends StoredValue


case class StoredNode(id: Long, labelIds: Array[Int]=null) extends StoredValue {
}

class StoredNodeWithProperty(override val id: Long,
                             override val labelIds: Array[Int],
                             val properties:Map[Int,Any])
  extends StoredNode(id, labelIds){
}

trait NodeStoreSPI {
  def allLabels(): Array[String];



  def allLabelIds(): Array[Int];

  def getLabelName(labelId: Int): Option[String];

  def getLabelId(labelName: String): Int;

  def addLabel(labelName: String): Int;

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Int;

  def addPropertyKey(keyName: String): Int;

  def getNodeById(nodeId: Long): Option[StoredNodeWithProperty];

  def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty];

  def getNodeIdsByLabel(labelId: Int): Iterator[Long];

  def getNodeLabelsById(nodeId: Long): Array[Int];

  def hasLabel(nodeId: Long, label: Int): Boolean;

  //  def getNodeLabelIdsBytes(nodeId: Long): Array[Byte];
  //
  //  def getNodePropertiesBytes(nodeId: Long): Array[Byte];

  def newNodeId(): Long;

  def nodeAddLabel(nodeId: Long, labelId: Int): Unit;

  def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit;

  def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any;

  def deleteNode(nodeId: Long): Unit;


  def serializeLabelIdsToBytes(labelIds: Array[Int]): Array[Byte] = {
    BaseSerializer.intArray2Bytes(labelIds)
  }

  def deserializeBytesToLabelIds(bytes: Array[Byte]): Array[Int] = {
    BaseSerializer.bytes2IntArray(bytes)
  }

  def serializePropertiesToBytes(properties: Map[Int, Any]): Array[Byte] = {
    BaseSerializer.map2Bytes(properties)
  }

  def deserializeBytesToProperties(bytes: Array[Byte]): Map[Int, Any] = {
    BaseSerializer.bytes2Map(bytes)
  }

  def allNodes(): Iterator[StoredNodeWithProperty]

  def nodesCount: Long

  def deleteNodesByLabel(labelId: Int): Unit

  def addNode(node: StoredNodeWithProperty): Unit

  def getLabelIds(labelNames: Set[String]): Set[Int]

  def close(): Unit
}