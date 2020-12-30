package cn.pandadb.kernel.kv

import org.opencypher.lynx.ir.{IRContextualNodeRef, IRNode, IRNodeRef, IRRelation, IRStoredNodeRef, PropertyGraphWriter}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue, Node, Relationship}
case class TestNode(id: Long, labels: Set[String], props: (String, CypherValue)*) extends Node[Long] {
  lazy val properties = props.toMap
  val withIds = props.toMap + ("_id" -> CypherValue(id))
  override type I = this.type

  override def copy(id: Long, labels: Set[String], properties: CypherMap): TestNode.this.type = this
}

case class TestRelationship(id: Long, startId: Long, endId: Long, relType: String, props: (String, CypherValue)*) extends Relationship[Long] {
  val properties = props.toMap
  val withIds = props.toMap ++ Map("_id" -> CypherValue(id), "_from" -> CypherValue(startId), "_to" -> CypherValue(endId))
  override type I = this.type

  override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): TestRelationship.this.type = this
}
class PandaPropertyGraphWriterImpl extends PropertyGraphWriter[Long]{

  def generateNodeId(): Long = ???

  def generateRelId(): Long = ???

  def addNode(node: TestNode) = ???
  def addRel(rel: TestRelationship) = ???
  override def createElements(nodes: Array[IRNode], rels: Array[IRRelation[Long]]): Unit = {

    val nodesMap = nodes.map(node => {
      val id = generateNodeId
      node -> id.toLong
    }).toMap

    def nodeId(ref: IRNodeRef[Long]) = {
      ref match {
        case IRStoredNodeRef(id) => id
        case IRContextualNodeRef(node) => nodesMap(node)
      }
    }

    nodesMap.map(x => TestNode(x._2, x._1.labels.toSet, x._1.props:_*)).map(addNode)

    rels.map(rel => TestRelationship(generateRelId, startId = nodeId(rel.startNodeRef), endId = nodeId(rel.endNodeRef), rel.types.head, rel.props: _*)).map(addRel)
  }
}
