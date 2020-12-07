package cn.pandadb.kernel.optimizer

import org.opencypher.lynx.planning.{LynxPhysicalOptimizer, LynxPhysicalPlanner, PhysicalOperator, SimpleTableOperator}
import org.opencypher.lynx.{LynxPlannerContext, LynxResult, LynxSession, TableOperator}
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.ir.api.CypherQuery
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.logical.impl._

class PandaCypherSession extends LynxSession{
  override protected val _parser: CypherParser = CypherParser
  override protected val _tableOperator: TableOperator = new SimpleTableOperator
  override protected val _createLogicalPlan: (CypherQuery, LogicalPlannerContext) => LogicalOperator =
    (ir: CypherQuery, context: LogicalPlannerContext) => new LogicalPlanner(new LogicalOperatorProducer).process(ir)(context)
  override protected val _optimizeLogicalPlan: (LogicalOperator, LogicalPlannerContext) => LogicalOperator =
    (input: LogicalOperator, context: LogicalPlannerContext) => LogicalOptimizer.process(input)(context)
  override protected val _createPhysicalPlan: (LogicalOperator, LynxPlannerContext) => PhysicalOperator =
    (input: LogicalOperator, context: LynxPlannerContext) => LynxPhysicalPlanner.process(input)(context)
  override protected val _optimizePhysicalPlan: (PhysicalOperator, LynxPlannerContext) => PhysicalOperator =
    (input: PhysicalOperator, context: LynxPlannerContext) => PandaPhysicalOptimizer.process(input)(context)
  override protected val _createCypherResult: (PhysicalOperator, LogicalOperator, LynxPlannerContext) => LynxResult =
    (input: PhysicalOperator, logical: LogicalOperator, context: LynxPlannerContext) => LynxResult(input, logical)

  override protected def getOrUpdateLogicalPlan(graph: PropertyGraph, cypherQuery: CypherQuery, parameters: CypherValue.CypherMap, logicalPlan: () => LogicalOperator): LogicalOperator = super.getOrUpdateLogicalPlan(graph, cypherQuery, parameters, logicalPlan)
}



