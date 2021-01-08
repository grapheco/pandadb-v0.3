package cn.pandadb.hipporpc.utils

import cn.pandadb.hipporpc.values.{BooleanValue, DateValue, FloatValue, IntegerValue, Label, LocalDateTimeValue, Node, NodeValue, NullValue, Relationship, RelationshipType, RelationshipValue, StringValue, Value}
import org.opencypher.okapi.api.value.CypherValue

import scala.collection.mutable

class ValueConverter {
  def converterValue(v: Any): Value = {
    v match {
      case node:CypherValue.Node[Long] => convertNode(node)
      case relationship: CypherValue.Relationship[Long] => convertRelationship(relationship)
      case i: CypherValue.CypherInteger => IntegerValue(i.value)
      case f: CypherValue.CypherFloat => FloatValue(f.value)
      case str: CypherValue.CypherString => StringValue(str.value)
      case b: CypherValue.CypherBoolean => BooleanValue(b.value)
      case date: CypherValue.CypherDate => DateValue(date.value)
      case localDateTime: CypherValue.CypherLocalDateTime => LocalDateTimeValue(localDateTime.value)
      case _ => NullValue
//      case number: CypherValue.CypherNumber =>
//      case duration: CypherValue.CypherDuration => values.DurationValue(duration.value)
    }
  }

  def convertNode(node: CypherValue.Node[Long]): NodeValue ={
    val id = node.id
    val labels = node.labels.toArray.map(l => Label(l))
    val properties = node.properties
    val propertiesMap = new mutable.HashMap[String, Value]()
    for (k <- properties.keys){
      val v1 = properties.getOrElse(k, null)
      val v: Value = converterValue(v1)
      propertiesMap(k) = v
    }
    val convertedNode = Node(id, propertiesMap.toMap, labels)
    NodeValue(convertedNode)
  }

  def convertRelationship(relationship: CypherValue.Relationship[Long]): RelationshipValue ={
    val id = relationship.id
    val relationshipType = RelationshipType(relationship.relType)
    val startNodeId = relationship.startId
    val endNodeId = relationship.endId

    val properties = relationship.properties
    val propertiesMap = new mutable.HashMap[String, Value]()
    for (k <- properties.keys){
      val v1 = properties.getOrElse(k, null)
      val v: Value = converterValue(v1)
      propertiesMap(k) = v
    }

    val rel = Relationship(id, propertiesMap.toMap, startNodeId, endNodeId, relationshipType)
    RelationshipValue(rel)
  }
}
