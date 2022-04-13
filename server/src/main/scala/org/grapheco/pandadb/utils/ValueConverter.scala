package org.grapheco.pandadb.utils

import org.grapheco.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.grapheco.pandadb.net.rpc.values.{BooleanValue, FloatValue, IntegerValue, Label, ListValue, Node, NodeValue, NullValue, Relationship, RelationshipType, RelationshipValue, StringValue, Value}
import org.grapheco.lynx.{LynxBoolean, LynxDouble, LynxInteger, LynxList, LynxString, version}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

class ValueConverter {
  def converterValue(v: Any): Value = {
    v match {
      case pandaNode: PandaNode => convertPandaNode(pandaNode)
      case pandaRelationship: PandaRelationship =>convertPandaRelationship(pandaRelationship)
      case lynxInteger: LynxInteger => IntegerValue(lynxInteger.value)
      case lynxString: LynxString => StringValue(lynxString.value)
      case lynxDouble: LynxDouble => FloatValue(lynxDouble.value)
      case lynxBoolean: LynxBoolean => BooleanValue(lynxBoolean.value)
      case lynxList: LynxList => converterLynxList(lynxList)
      case v: String => StringValue(v)
      case v: Int => IntegerValue(v)
      case v: Long => IntegerValue(v)
      case v: Double => FloatValue(v)
      case v: Boolean => BooleanValue(v)
      case v: Seq[Any] =>converterNormalList(v)
      case _ => NullValue
    }
  }

  def convertPandaNode(node: PandaNode): NodeValue ={
    val id = node.id.value
    val labels = node.labels.toArray.map(l => Label(l.value))
    val properties = node.props.map{ case (key, value) => (key.value, value)}
    val propertiesMap = new mutable.HashMap[String, Value]()
    for (k <- properties.keys){
      val v1 = properties.getOrElse(k, null)
      val v: Value = converterValue(v1)
      propertiesMap(k) = v
    }
    val convertedNode = Node(id, propertiesMap.toMap, labels)
    NodeValue(convertedNode)
  }

  def convertPandaRelationship(relationship: PandaRelationship): RelationshipValue ={
    val id = relationship.id.value
    val relationshipType = RelationshipType(relationship.relationType.get.value)
    val startNodeId = relationship.startNodeId.value
    val endNodeId = relationship.endNodeId.value

    val properties = relationship.props.map{ case (key, value) => (key.value, value)}
    val propertiesMap = new mutable.HashMap[String, Value]()
    for (k <- properties.keys){
      val v1 = properties.getOrElse(k, null)
      val v: Value = converterValue(v1)
      propertiesMap(k) = v
    }

    val rel = Relationship(id, propertiesMap.toMap, startNodeId, endNodeId, relationshipType)
    RelationshipValue(rel)
  }

  def converterLynxList(lynxList: LynxList): ListValue ={
    ListValue(lynxList.value.map(f => converterValue(f)).toBuffer.asInstanceOf[ArrayBuffer[Value]])
  }
  def converterNormalList(list: Seq[Any]): ListValue = {
    ListValue(list.map(f => converterValue(f)).toBuffer.asInstanceOf[ArrayBuffer[Value]])
  }
}
