package org.grapheco.lynx

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}
import org.grapheco.lynx.DataFrameOps._

import scala.collection.mutable.ArrayBuffer

class PhysicalPlannerImpl()(implicit runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  var retItem: Seq[ReturnItem] = _
  override def plan(logicalPlan: LogicalPlanNode): PhysicalPlanNode = {
    logicalPlan match {
      case LogicalProcedureCall(c: UnresolvedCall) => PhysicalProcedureCall(c)
      case LogicalCreate(c: Create, in: Option[LogicalQueryClause]) => PhysicalCreate(c, in.map(plan(_)))
      case LogicalMatch(m: Match, in: Option[LogicalQueryClause]) => {
        if (retItem.isEmpty) PhysicalMatch1(m, in.map(plan(_)))
        else PhysicalMatch1(m, in.map(plan(_)), retItem)
      }
      case LogicalReturn(r: Return, in: Option[LogicalQueryClause]) => {
        this.retItem = r.returnItems.items
        PhysicalReturn(r, in.map(plan(_)))
      }
      case LogicalWith(w: With, in: Option[LogicalQueryClause]) => PhysicalWith(w, in.map(plan(_)))
      case LogicalQuery(LogicalSingleQuery(in)) => PhysicalSingleQuery(in.map(plan(_)))
    }
  }
}

case class PhysicalMatch1(m: Match, in: Option[PhysicalPlanNode], retItem: Seq[ReturnItem] = null)(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame = {
    //run match
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    val df = patternParts match {
      case Seq(EveryPath(element: PatternElement)) =>
        patternMatch(element)(ctx)
    }

    where match {
      case Some(Where(condition)) => df.filter(condition)(ctx.expressionContext)
      case None => df
    }
  }

  private def patternMatch(element: PatternElement)(ctx: PlanExecutionContext): DataFrame = {
    element match {
      //match (m:label1)
      case NodePattern(
      Some(var0: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]) =>
        DataFrame(Seq(var0.name -> CTNode), () => {
          val nodes = if (labels.isEmpty)
            runnerContext.graphModel.nodes()
          else
            runnerContext.graphModel.nodes(labels.map(_.name), false)

          nodes.map(Seq(_))
        })

      //match (m:label1)-[r:type]->(n:label2)
      case RelationshipChain(
      leftNode@NodePattern(var1, labels1: Seq[LabelName], properties1: Option[Expression], baseNode1: Option[LogicalVariable]),
      RelationshipPattern(variable: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
      rightNode@NodePattern(var2, labels2: Seq[LabelName], properties2: Option[Expression], baseNode2: Option[LogicalVariable])
      ) =>
        DataFrame((variable.map(_.name -> CTRelationship) ++ ((var1 ++ var2).map(_.name -> CTNode))).toSeq, () => {
          val var1IsRet: Boolean = if(var1.isDefined) retItem.map(_.name).contains(var1.get.name) else false
          val var2IsRet: Boolean = if(var2.isDefined) retItem.map(_.name).contains(var2.get.name) else false
          val isRelRet: Boolean = if(variable.isDefined) retItem.map(_.name).contains(variable.get.name) else false
          val rels: Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] =
            runnerContext.graphModel.rels(types.map(_.name), labels1.map(_.name), labels2.map(_.name), var1IsRet, var2IsRet)
          rels.flatMap {
            rel => {
              val (v0, v1, v2) = rel
              direction match {
                case BOTH =>
                  Iterator.apply(
                    Seq(v0) ++ var1.map(_ => v1.get) ++ var2.map(_ => v2.get),
                    Seq(v0) ++ var1.map(_ => v2.get) ++ var2.map(_ => v1.get)
                  )
                case INCOMING =>
                  Iterator.single(Seq(v0) ++ var1.map(_ => v2.get) ++ var2.map(_ => v1.get))
                case OUTGOING =>
                  Iterator.single(Seq(v0) ++ var1.map(_ => v1.get) ++ var2.map(_ => v2.get))
              }
            }
          }
        })

      //match ()-[]->()-...-[r:type]->(n:label2)
      /*
    case RelationshipChain(
    leftChain,
    RelationshipPattern(variable: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
    rightNode@NodePattern(var2, labels2: Seq[LabelName], properties2: Option[Expression], baseNode2: Option[LogicalVariable])
    ) =>
      val in = patternMatch(leftChain)
      DataFrame((variable.map(_.name -> CTRelationship) ++ ((var1 ++ var2).map(_.name -> CTNode))).toSeq, () => {
        val rels: Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] =
          runnerContext.graphModel.rels(types.map(_.name), labels1.map(_.name), labels2.map(_.name), var1.isDefined, var2.isDefined)
        rels.flatMap {
          rel => {
            val (v0, v1, v2) = rel
            direction match {
              case BOTH =>
                Iterator.apply(
                  Seq(v0) ++ var1.map(_ => v1.get) ++ var2.map(_ => v2.get),
                  Seq(v0) ++ var1.map(_ => v2.get) ++ var2.map(_ => v1.get)
                )
              case INCOMING =>
                Iterator.single(Seq(v0) ++ var1.map(_ => v2.get) ++ var2.map(_ => v1.get))
              case OUTGOING =>
                Iterator.single(Seq(v0) ++ var1.map(_ => v1.get) ++ var2.map(_ => v2.get))
            }
          }
        }
      })

       */
    }
  }
}