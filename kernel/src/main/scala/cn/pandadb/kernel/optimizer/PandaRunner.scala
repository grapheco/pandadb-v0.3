//package cn.pandadb.kernel.optimizer
//
//import org.grapheco.lynx.util.FormatUtils
//import org.grapheco.lynx.{CypherRunner, GraphModel, LogicalPlanNode, LynxResult, PhysicalPlanNode, PlanAware, PlanExecutionContext}
//import org.opencypher.v9_0.ast.Statement
//
//class PandaRunner(graphModel: GraphModel) extends CypherRunner(graphModel){
//
//  override def run(query: String, param: Map[String, Any]): LynxResult = {
//    val (statement, param2, state) = queryParser.parse(query)
//    logger.debug(s"AST tree: ${statement}")
//
//    val logicalPlan = logicalPlanner.plan(statement)
//    logger.debug(s"logical plan: ${logicalPlan}")
//
//    val physicalPlan = physicalPlanner.plan(logicalPlan)
//    logger.debug(s"physical plan: ${physicalPlan}")
//
//    val ctx = PlanExecutionContext(param ++ param2)
//    val df = physicalPlan.execute(ctx)
//
//    new LynxResult() with PlanAware {
//      val schema = df.schema
//      val columnNames = schema.map(_._1)
//
//      override def show(limit: Int): Unit =
//        FormatUtils.printTable(columnNames,
//          df.records.take(limit).toSeq.map(_.map(_.value)))
//
//      override def columns(): Seq[String] = columnNames
//
//      override def records(): Iterator[Map[String, Any]] = df.records.map(columnNames.zip(_).toMap)
//
//      override def getASTStatement(): (Statement, Map[String, Any]) = (statement, param2)
//
//      override def getLogicalPlan(): LogicalPlanNode = logicalPlan
//
//      override def getPhysicalPlan(): PhysicalPlanNode = physicalPlan
//    }
//  }
//}
