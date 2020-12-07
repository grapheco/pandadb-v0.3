package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.kv.{AnyValue, NFEquals, NFGreaterThan, NFGreaterThanOrEqual, NFLabels, NFLessThan, NFLessThanOrEqual, NFPredicate}
import org.opencypher.lynx.graph.LynxPropertyGraph
import org.opencypher.lynx.{LynxRecords, LynxTable, RecordHeader}
import org.opencypher.lynx.planning.{Filter, LabelRecorders, PhysicalOperator}
import org.opencypher.okapi.api.types.{CTNode, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue, Node}
import org.opencypher.okapi.ir.api.expr.{ElementProperty, Equals, Expr, GreaterThan, GreaterThanOrEqual, Id, LessThan, LessThanOrEqual, NodeVar, Param}

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer


object PpdFilter {
  def expr2predicate(expr: Expr, parameters: CypherMap): NFPredicate = {
    //todo transform more Exprs to NFPredicates
    expr match {
      case GreaterThan(lhs, rhs) => NFGreaterThan(lhs.asInstanceOf[ElementProperty].key.name, AnyValue(parameters.get(rhs.asInstanceOf[Param].name).get.getValue.get))
      case Equals(lhs, rhs) => NFEquals(lhs.asInstanceOf[ElementProperty].key.name, AnyValue(parameters.get(rhs.asInstanceOf[Param].name).get.getValue.get))
      case LessThan(lhs, rhs) => NFLessThan(lhs.asInstanceOf[ElementProperty].key.name, AnyValue(parameters.get(rhs.asInstanceOf[Param].name).get.getValue.get))
      case LessThanOrEqual(lhs, rhs) => NFLessThanOrEqual(lhs.asInstanceOf[ElementProperty].key.name, AnyValue(parameters.get(rhs.asInstanceOf[Param].name).get.getValue.get))
      case GreaterThanOrEqual(lhs, rhs) => NFGreaterThanOrEqual(lhs.asInstanceOf[ElementProperty].key.name, AnyValue(parameters.get(rhs.asInstanceOf[Param].name).get.getValue.get))
    }
  }
  def getPredicate(op: PhysicalOperator): NFPredicate = {
    op match {
      case x:LabelRecorders => {
        NFLabels(x.cypherType.asInstanceOf[CTNode].labels.toSeq)
      }
      case x:Filter => {
        expr2predicate(x.expr, x.context.parameters)
      }
    }
  }

  def getNodeVar(in: PhysicalOperator): (String, CypherType) = {
    in match {
      case x: Filter =>
        x.expr match {
          case x: GreaterThan =>
            x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].name ->
              x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].cypherType
          case x: Equals =>
            x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].name ->
              x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].cypherType
          case x: LessThan =>
            x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].name ->
              x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].cypherType
          case x: LessThanOrEqual =>
            x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].name ->
              x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].cypherType
          case x: GreaterThanOrEqual =>
            x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].name ->
              x.lhs.asInstanceOf[ElementProperty].propertyOwner.asInstanceOf[NodeVar].cypherType
        }
      case x: LabelRecorders => x.name -> x.cypherType.asInstanceOf[CTNode]
    }
  }
}

object costore {
  case class LynxNode(id: Long, labels: Set[String], props: (String, CypherValue)*) extends Node[Long] {
    //lazy val properties = props.toMap
    val withIds = props.toMap + ("_id" -> CypherValue(id))
    override type I = this.type

    override def copy(id: Long, labels: Set[String], properties: CypherMap): LynxNode.this.type = this

    override def properties: CypherMap = props.toMap
  }

  //def reorder

  def getLynxNodeFromCostore(prediates: Array[NFPredicate], graph:LynxPropertyGraph): Iterable[LynxNode] = {
    //todo getnodes from costore according to predicates
    val node1 = LynxNode(1, Set("person", "t1"), "name" -> CypherValue("bluejoe"), "age" -> CypherValue(40))
    Array(node1)
  }
  def getRecordersFromPredicates(prediates: Array[NFPredicate], name: String, nodeCypherType: CTNode, graph:LynxPropertyGraph): LynxRecords = {
    val nodes = getLynxNodeFromCostore(prediates, graph)
    LynxRecords(
    RecordHeader(Map(NodeVar(name)(CTNode) -> name)),
    LynxTable(Seq(name -> CTNode), Seq(nodes.toSeq)))
  }
}

case class PpdFilter(ops: ArrayBuffer[PhysicalOperator], in: PhysicalOperator) extends PhysicalOperator {
  lazy val prediates = ArrayBuffer[NFPredicate]()
  ops.foreach(u =>{
    prediates += PpdFilter.getPredicate(u)
  })

  //override lazy val graph:LynxPropertyGraph  = in.graph

  lazy  val (name, ctype) = PpdFilter.getNodeVar(ops.head)
  lazy val recorders = costore.getRecordersFromPredicates(prediates.toArray, name, ctype.asInstanceOf[CTNode], this.graph)
  override lazy val _table: LynxTable = recorders.table
  override lazy val recordHeader: RecordHeader = recorders.header
  //override lazy val recordHeader: RecordHeader = ops.head.recordHeader
  val test = 89

}
