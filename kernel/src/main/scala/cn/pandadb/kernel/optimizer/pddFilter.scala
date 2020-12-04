package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.kv.{AnyValue, NFEquals, NFGreaterThan, NFGreaterThanOrEqual, NFLabels, NFLessThan, NFLessThanOrEqual, NFPredicate}
import cn.pandadb.kernel.optimizer.PandaPhysicalOptimizer.getPredicate
import org.opencypher.lynx.{LynxTable, RecordHeader}
import org.opencypher.lynx.planning.{Filter, LabelRecorders, PhysicalOperator}
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{ElementProperty, Equals, Expr, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Param}

import scala.collection.mutable.ArrayBuffer


object ppdFilter {
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
}

object costore {
  def getLynxTableFromPredicates(prediates: Array[NFPredicate]): (LynxTable, RecordHeader) = {
    LynxTable.empty() -> RecordHeader.empty
  }
}

case class ppdFilter(ops: ArrayBuffer[PhysicalOperator], in: PhysicalOperator) extends PhysicalOperator {
  lazy val prediates = ArrayBuffer[NFPredicate]()
  ops.foreach(u =>{
    prediates += ppdFilter.getPredicate(u)
  })

  override lazy val _table: LynxTable = costore.getLynxTableFromPredicates(prediates.toArray)._1
  override lazy val recordHeader: RecordHeader = costore.getLynxTableFromPredicates(prediates.toArray)._2


}
