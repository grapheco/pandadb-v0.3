package cn.pandadb.kernel.store

import cn.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.grapheco.lynx.{LynxId, LynxNode, LynxValue}


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

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[StoredNodeWithProperty]
    if (id == other.id && labelIds.sameElements(other.labelIds) && properties.sameElements(other.properties)) true
    else false
  }

  override def toString: String = s"{<id>:${id}, labels:[${labelIds.mkString(",")}], properties:{${properties.map(kv=>kv._1+": "+kv._2.toString).mkString(",")}}"
}

case class NodeId(value: Long) extends LynxId {}

case class PandaNode(longId: Long, labels: Seq[String], props: (String, LynxValue)*) extends LynxNode {
  lazy val properties: Map[String, LynxValue] = props.toMap
  override val id: LynxId = NodeId(longId)
  override def property(name: String): Option[LynxValue] = properties.get(name)

  override def toString: String = s"{<id>:${id.value}, labels:[${labels.mkString(",")}], properties:{${properties.map(kv=>kv._1+": "+kv._2.value.toString).mkString(",")}}"
}

case class LazyPandaNode(longId: Long, nodeStoreSPI: DistributedNodeStoreSPI) extends LynxNode {
  lazy val nodeValue: PandaNode = transfer(nodeStoreSPI)
  override val id: LynxId = NodeId(longId)

  override def labels: Seq[String] = nodeStoreSPI.getNodeLabelsById(longId).map(f=>nodeStoreSPI.getLabelName(f).get).toSeq

  override def property(name: String): Option[LynxValue] = nodeValue.properties.get(name)

  def transfer(nodeStore: DistributedNodeStoreSPI): PandaNode = {
    val node = nodeStore.getNodeById(longId).get
    PandaNode(node.id,
      node.labelIds.map((id: Int) => nodeStore.getLabelName(id).get).toSeq,
      node.properties.map(kv=>(nodeStore.getPropertyKeyName(kv._1).getOrElse("unknown"), LynxValue(kv._2))).toSeq:_*)
  }
}