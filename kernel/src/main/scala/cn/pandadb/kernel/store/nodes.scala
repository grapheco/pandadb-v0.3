package cn.pandadb.kernel.store

import cn.pandadb.kernel.kv.node.NodeLabelStore
import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.grapheco.lynx.{LynxId, LynxNode, LynxNull, LynxValue}


trait StoredValue{
}

case class StoredValueNull() extends StoredValue

case class StoredNode(id: Long, labelIds: Array[Int]) extends StoredValue {
  val properties:Map[Int,Any] = Map.empty
}

class StoredNodeWithProperty(override val id: Long,
                             override val labelIds: Array[Int],
                             override val properties:Map[Int,Any])
  extends StoredNode(id, labelIds){
}

case class NodeId(value: Long) extends LynxId {}

case class PandaNode(longId: Long, labels: Seq[String], props: (String, LynxValue)*) extends LynxNode {
  lazy val properties: Map[String, LynxValue] = props.toMap
  override val id: LynxId = NodeId(longId)
  override def property(name: String): Option[LynxValue] = properties.get(name)

  override def toString: String = s"{<id>:${id.value}, labels:[${labels.mkString(",")}], properties:{${properties.map(kv=>kv._1+": "+kv._2.value.toString).mkString(",")}}"
}

case class LazyPandaNode(longId: Long, nodeStoreSPI: NodeStoreSPI) extends LynxNode {
  lazy val nodeValue: PandaNode = transfer(nodeStoreSPI)
  override val id: LynxId = NodeId(longId)

  override def labels: Seq[String] = nodeStoreSPI.getNodeLabelsById(longId).map(f=>nodeStoreSPI.getLabelName(f).get).toSeq

  override def property(name: String): Option[LynxValue] = nodeValue.properties.get(name)

  def transfer(nodeStore: NodeStoreSPI): PandaNode = {
    val node = nodeStore.getNodeById(longId).get
    PandaNode(node.id,
      node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq,
      node.properties.map(kv=>(nodeStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq:_*)
  }
}

trait NodeStoreSPI {
  def allLabels(): Array[String];

  def allLabelIds(): Array[Int];

  def getLabelName(labelId: Int): Option[String];

  def getLabelId(labelName: String): Option[Int];

  def addLabel(labelName: String): Int;

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def addPropertyKey(keyName: String): Int;

  def getNodeById(nodeId: Long): Option[StoredNodeWithProperty]

  def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty]

  def getNodeById(nodeId: Long, label: Option[Int]): Option[StoredNodeWithProperty]

  def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty];

  def getNodeIdsByLabel(labelId: Int): Iterator[Long];

  def getNodeLabelsById(nodeId: Long): Array[Int];

  def hasLabel(nodeId: Long, label: Int): Boolean;

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