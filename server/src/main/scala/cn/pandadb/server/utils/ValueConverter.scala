package cn.pandadb.server.utils

import cn.pandadb.server.values.{Label, RelationshipType, Value}
import org.opencypher.okapi.api.value.CypherValue
import cn.pandadb.server.values

import scala.collection.mutable
class ValueConverter {
  def converterValue(v: Any): Value = {
    v match {
      case node:CypherValue.Node[Long] => convertNode(node)
      case relationship: CypherValue.Relationship[Long] => convertRelationship(relationship)
      case i: CypherValue.CypherInteger => values.IntegerValue(i.value)
      case f: CypherValue.CypherFloat => values.FloatValue(f.value)
      case str: CypherValue.CypherString => values.StringValue(str.value)
      case b: CypherValue.CypherBoolean => values.BooleanValue(b.value)
      case _ => values.NullValue
//      case number: CypherValue.CypherNumber =>
//      case duration: CypherValue.CypherDuration => values.DurationValue(duration.value)
    }
  }

  def convertNode(node: CypherValue.Node[Long]): values.NodeValue ={
    val id = node.id
    val labels = node.labels.toArray.map(l => Label(l))
    val properties = node.properties
    val propertiesMap = new mutable.HashMap[String, Value]()
    for (k <- properties.keys){
      val v1 = properties.getOrElse(k, null)
      val v: Value = converterValue(v1)
      propertiesMap(k) = v
    }
    val convertedNode = values.Node(id, propertiesMap.toMap, labels)
    values.NodeValue(convertedNode)
  }

  def convertRelationship(relationship: CypherValue.Relationship[Long]): values.RelationshipValue ={
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

    val rel = values.Relationship(id, propertiesMap.toMap, startNodeId, endNodeId, relationshipType)
    values.RelationshipValue(rel)
  }
}
