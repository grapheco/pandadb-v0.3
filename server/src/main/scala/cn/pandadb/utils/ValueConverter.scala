package cn.pandadb.utils

import java.time.LocalDate

import cn.pandadb.hipporpc.values._
import cn.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.grapheco.lynx.{LynxBoolean, LynxDouble, LynxInteger, LynxList, LynxString}

import scala.collection.mutable.ArrayBuffer
//import org.opencypher.okapi.api.value.CypherValue

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
      case _ => NullValue
    }
  }

  def convertPandaNode(node: PandaNode): NodeValue ={
    val id = node.longId
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

  def convertPandaRelationship(relationship: PandaRelationship): RelationshipValue ={
    val id = relationship._id
    val relationshipType = RelationshipType(relationship.relationType.get)
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

  def converterLynxList(lynxList: LynxList): ListValue ={
    var newList = new ArrayBuffer[Value]()
    lynxList.value.foreach(lv => {
        newList += converterValue(lv)
    })
    ListValue(newList)
  }
}
