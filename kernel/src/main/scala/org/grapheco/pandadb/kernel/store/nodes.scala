package org.grapheco.pandadb.kernel.store

import org.grapheco.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import org.grapheco.pandadb.kernel.util.serializer.BaseSerializer
import org.grapheco.lynx.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxValue}


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

case class NodeId(value: Long) extends LynxId

case class PandaNode(id: NodeId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]) extends LynxNode{

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

  override def toString: String = s"{<id>:${id.value}, labels:[${labels.map(_.value).mkString(",")}], properties:{${props.map(kv=>kv._1.value+": "+kv._2.value.toString).mkString(",")}}"
}

case class IndexNode(id: String, labels: Seq[String], props: Map[String, Any])