package cn.pandadb.kernel.optimizer

import org.opencypher.lynx.graph.LynxPropertyGraph
import org.opencypher.lynx.plan.{LynxPhysicalPlanner, PandaPhysicalPlanner, PhysicalOperator}
import org.opencypher.lynx.planning.PandaTableOperator
import org.opencypher.lynx.{LynxPlannerContext, LynxResult, LynxSession, TableOperator}
import org.opencypher.okapi.logical.impl._

class PandaCypherSession extends LynxSession{
  private implicit val session: LynxSession = this

  override protected val _tableOperator: TableOperator = new PandaTableOperator

  override protected val _createPhysicalPlan: (LogicalOperator, LynxPlannerContext) => PhysicalOperator =
//    (input: LogicalOperator, context: LynxPlannerContext) => PandaPhysicalPlanner.process(input)(context)
    (input: LogicalOperator, context: LynxPlannerContext) => LynxPhysicalPlanner.process(input)(context)

  override protected val _optimizePhysicalPlan: (PhysicalOperator, LynxPlannerContext) => PhysicalOperator =
    (input: PhysicalOperator, context: LynxPlannerContext) => PandaPhysicalOptimizer.process(input)(context)

  def createPropertyGraph[Id](scan: PandaPropertyGraphScan[Id]): LynxPropertyGraph = new PandaPropertyGraph[Id](scan)(session)
}



