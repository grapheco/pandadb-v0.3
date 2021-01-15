package cn.pandadb.driver.utils

import cn.pandadb.hipporpc.values.{Value => HippoValue}
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.neo4j.driver.internal.value.{BooleanValue, DateTimeValue, FloatValue, IntegerValue, LocalDateTimeValue, NodeValue, RelationshipValue, StringValue}
import org.neo4j.driver.v1.{Record, Value => Neo4jValue}

import scala.collection.JavaConverters._

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
    }
  }

}
