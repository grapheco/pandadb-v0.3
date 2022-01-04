package cn.pandadb.driver.utils

import cn.pandadb.net.hipporpc.values.{Value => HippoValue}
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.neo4j.driver.internal.value.{BooleanValue, DateTimeValue, FloatValue, IntegerValue, ListValue, LocalDateTimeValue, NodeValue, NullValue, RelationshipValue, StringValue}
import org.neo4j.driver.v1.{Record, Value => Neo4jValue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object TypesToNeo4jValue {
  def getNeo4jValue(value:HippoValue): Neo4jValue  ={
    Types.withName(value.getType()) match {
      case Types.NODE => {
        val hippoNode = value.asNode()
        val labels = asJavaCollection[String](hippoNode.labels.map(l => l.name))
        val props = hippoNode.props.map(
          kv => (kv._1, getNeo4jValue(kv._2))
        )
        val neo4jNode = new InternalNode(hippoNode.id, labels, props.asJava)
        new NodeValue(neo4jNode)
      }
      case Types.RELATIONSHIP =>{
        val hippoRelationship = value.asRelationship()
        val props = hippoRelationship.props.map(
          kv => (kv._1, getNeo4jValue(kv._2))
        )
        val neo4jRelationship = new InternalRelationship(hippoRelationship.id, hippoRelationship.startNodeId,
          hippoRelationship.endNodeId, hippoRelationship.relationshipType.name, props.asJava)
        new RelationshipValue(neo4jRelationship)
      }
      case Types.BOOLEAN => BooleanValue.fromBoolean(value.asBoolean())
      case Types.INTEGER => new IntegerValue(value.asLong())
      case Types.STRING => new StringValue(value.asString())
      case Types.FLOAT => new FloatValue(value.asFloat())
      case Types.DATE_TIME => new DateTimeValue(value.asDateTime())
      case Types.DATE => new DateTimeValue(value.asDateTime())
      case Types.LOCAL_DATE_TIME => new LocalDateTimeValue(value.asLocalDateTime())
      case Types.LIST => {
        val hippoList = value.asList()
        val neo4jList = new ArrayBuffer[Neo4jValue]()
        hippoList.foreach(hv => {
          neo4jList += getNeo4jValue(hv)
        })
        new ListValue(neo4jList.seq: _*)
      }
      case Types.NULL =>{
        NullValue.NULL
      }
      case any => throw new NotSupportValueTypeException(s"not support ${any.toString} type")
    }
  }
}

class NotSupportValueTypeException(s: String) extends Exception {
  override def getMessage: String = s
}
