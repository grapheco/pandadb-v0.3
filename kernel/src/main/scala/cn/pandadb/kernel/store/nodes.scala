package cn.pandadb.kernel.store

import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.opencypher.v9_0.util.LabelId


trait ReadOnlyNode {

  def getId: Long;

  def getProperty(key: Int): Any;

  def getAllProperties: Map[Int, Any];

  def getLabels: Array[Int];
}

trait WritableNode extends ReadOnlyNode{
  def setProperty(key:Int, value: Any): Unit = ???;

  def removeProperty(key:Int): Any = ???;

  def addLabel(label: Int): Unit = ???;

  def removeLabel(label: Int): Unit = ???;
}

class StoredNode(id: Long, labelIds: Array[Int]=null) extends ReadOnlyNode {
  override def getId: Long = id

  override def getProperty(key: Int): Any = null

  override def getAllProperties: Map[Int, Any] = null

  override def getLabels: Array[Int] = labelIds
}

class StoredNodeWithProperty(id: Long, labelIds: Array[Int], properties:Map[Int,Any]) extends ReadOnlyNode {
  override def getId: Long = id

  override def getProperty(key: Int): Any = properties(key)

  override def getAllProperties: Map[Int, Any] = properties

  override def getLabels: Array[Int] = labelIds
}

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


trait NodeStoreSPI {
  def allLabels(): Array[String];
  def allLabelIds(): Array[Int];
  def getLabelName(labelId: Int): String;
  def getLabelId(labelName: String): Int;

  def addLabel(labelName: String): Int;

  def allPropertyKeys(): Array[String];
  def allPropertyKeyIds(): Array[Int];
  def getPropertyKeyName(keyId: Int): String;
  def getPropertyKeyId(keyName: String): Int;

  def addPropertyKey(keyName: String): Int;

  def getNodeById(nodeId: Long) : ReadOnlyNode;

  def addNode(node: StoredNodeWithProperty): Unit

  def getNodesByLabel(labelId: Int): Iterator[ReadOnlyNode];
  def getNodeIdsByLabel(labelId: Int): Iterator[Long];

  def nodeAddLabel(nodeId: Long, labelId:Int): Unit;
  def nodeRemoveLabel(nodeId: Long, labelId:Int): Unit;

  def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any;

  def deleteNode(nodeId: Long): Unit;
  def deleteNodesByLabel(labelId: Int): Unit

  def allNodes(): Iterator[ReadOnlyNode];

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

}