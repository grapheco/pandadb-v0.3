package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.kv.{AnyValue, NFEquals, NFGreaterThan, NFGreaterThanOrEqual, NFLabels, NFLessThan, NFLessThanOrEqual, NFPredicate}
import org.opencypher.lynx.graph.LynxPropertyGraph
import org.opencypher.lynx.{LynxPlannerContext, LynxTable, RecordHeader}
import org.opencypher.lynx.planning.{Add, AddInto, Aggregate, Alias, Cache, ConstructGraph, Distinct, Drop, EmptyRecords, Filter, FromCatalogGraph, GraphUnionAll, Join, Limit, OrderBy, PhysicalOperator, PrefixGraph, ReturnGraph, Select, Skip, Start, SwitchContext, TabularUnionAll}
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{ElementProperty, Equals, Expr, GreaterThan, GreaterThanOrEqual, Id, LessThan, LessThanOrEqual, Param}
import org.opencypher.okapi.trees.TopDown

import scala.collection.mutable.ArrayBuffer

object PandaPhysicalOptimizer {

  def process(input: PhysicalOperator)(implicit context: LynxPlannerContext): PhysicalOperator = {
    //InsertCachingOperators(input)
    filterPushDown(input)
  }

  def filterPushDown(input: PhysicalOperator): PhysicalOperator = {
    val newPlan: PhysicalOperator = extractFilter(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), input)
    if(newPlan==null) input
    else newPlan
  }

  def extractFilter(filterOps: ArrayBuffer[PhysicalOperator], ordinaryOps: ArrayBuffer[PhysicalOperator], input: PhysicalOperator): PhysicalOperator = {

    input match {
      case x: Filter => {
        filterOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      }
  /*    case x: LabelRecorders => {
        filterOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      }*/
      case x: Start =>
        generatePhysicalPlan(filterOps, ordinaryOps, x)
      case x: Select =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)

      case x: Add =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: AddInto =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: Aggregate =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: Alias =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: Cache =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: ConstructGraph =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: Distinct =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: Drop[Any] =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: EmptyRecords =>
        if (x.fields.size >= 1) filterOps += x
        else ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: FromCatalogGraph =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: GraphUnionAll =>
        generatePhysicalPlan(filterOps, ordinaryOps, x)
      case x: Join =>
        val op1 = filterPushDown(x.lhs)
        val op2 = filterPushDown(x.rhs)
        val join = Join(op1, op2, x.joinExprs, x.joinType)
        generatePhysicalPlan(filterOps, ordinaryOps, join)
      case x: Limit =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: OrderBy =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: PrefixGraph =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: ReturnGraph =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: Skip =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: SwitchContext =>
        ordinaryOps += x
        extractFilter(filterOps, ordinaryOps, x.in)
      case x: TabularUnionAll =>
        //val op1 = extractFilter(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), x.lhs)
        val op1 = filterPushDown(x.lhs)
        //val op2 = extractFilter(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), x.rhs)
        val op2 = filterPushDown(x.rhs)
        val tabularUnionAll = TabularUnionAll(op1, op2)
        generatePhysicalPlan(filterOps, ordinaryOps, tabularUnionAll)
    }
  }

  def isLabel(in: PhysicalOperator): Boolean = {
    in match {
      case x:EmptyRecords => true
      case _ => false
    }
  }

  def isNecessaryPPD(filterops: ArrayBuffer[PhysicalOperator], graph:LynxPropertyGraph): Boolean = {
    //val prediates = ArrayBuffer[NFPredicate]()
    //filterops.foreach(u =>{
    //  prediates += PpdFilter.getPredicate(u)
    filterops
   // })
    //todo figure out whether PPD is necessary
    val prediates = ArrayBuffer[NFPredicate]()
    filterops.foreach(u =>{
      prediates += PpdFilter.getPredicate(u)
    })

    filterops.map(isLabel(_)).reduce(_|_) //&& graph.asInstanceOf[PandaPropertyGraph[Id]].isNFPredicatesWithIndex(prediates.toArray)

    //graph.asInstanceOf[PandaPropertyGraph].isNFPredicatesWithIndex(prediates.toArray)
  }

  def reOrderPredicates(filterops: ArrayBuffer[PhysicalOperator], graph:LynxPropertyGraph): Array[NFPredicate] = {
    //todo reorder pddfilter according to graph node counts
    val prediates = ArrayBuffer[NFPredicate]()
    filterops.foreach(u =>{
      prediates += PpdFilter.getPredicate(u)
     })
    prediates.toArray
  }



  def generatePhysicalPlan(filterops: ArrayBuffer[PhysicalOperator], opseq: ArrayBuffer[PhysicalOperator], endOp: PhysicalOperator): PhysicalOperator = {
    if (!filterops.isEmpty) {
      if(isNecessaryPPD(filterops, endOp.graph)) {
        var tempOp: PhysicalOperator = PpdFilter(filterops, endOp, reOrderPredicates(filterops, endOp.graph))
        opseq.reverse.foreach(u => {
          tempOp = constructPhysicalPlan(u, tempOp)
        })
        tempOp
      }
      else null
    }
    else null

  }

  def constructPhysicalPlan(current: PhysicalOperator, in: PhysicalOperator): PhysicalOperator = {
    //todo with null
    current match {
      case x: Filter => null
      //case x: LabelRecorders => null
      case x: Start => null
      case x: Select => Select(in, x.expressions, x.columnRenames)
      case x: Add => Add(in, x.exprs)
      case x: AddInto => AddInto(in, x.valueIntoTuples)
      case x: Aggregate => Aggregate(in, x.group, x.aggregations)
      case x: Alias => Alias(in, x.aliases)
      case x: Cache => Cache(in)
      case x: ConstructGraph => ConstructGraph(in, x.constructedGraph, x.construct, x.context)
      case x: Distinct => Distinct(in, x.fields)
      case x: Drop[Expr] => Drop(in, x.exprs)
      case x: EmptyRecords => EmptyRecords(in, x.fields)
      case x: FromCatalogGraph => FromCatalogGraph(in, x.logicalGraph)
      case x: GraphUnionAll => null
      case x: Join => null
      case x: Limit => Limit(in, x.expr)
      case x: OrderBy => OrderBy(in, x.sortItems)
      case x: PrefixGraph => PrefixGraph(in, x.prefix)
      case x: ReturnGraph => ReturnGraph(in)
      case x: Skip => Skip(in, x.expr)
      case x: SwitchContext => {
        in match {
          case y:Select => y.in
          case _ => SwitchContext(in, x.context)
        }
      }
      case x: TabularUnionAll => null
    }

  }
  object InsertCachingOperators {

    def apply(input: PhysicalOperator): PhysicalOperator = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: Start => false
        case _ => true
      }

      val nodesToReplace = replacements.keySet

      TopDown[PhysicalOperator] {
        case cache: Cache => cache
        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren)
      }.transform(input)
    }

    private def calculateReplacementMap(input: PhysicalOperator): Map[PhysicalOperator, PhysicalOperator] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[PhysicalOperator] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache, currentCounts) =>
            val currentOpCount = currentCounts(currentOp)
            if (currentOpCount > 1) {
              val updatedOps = currentOpsToCache + currentOp
              // We're traversing `opsByHeight` from largest to smallest query sub-tree.
              // We pick the trees with the largest height for caching first, and then reduce the duplicate count
              // for the sub-trees of the cached tree by the number of times the parent tree appears.
              // The idea behind this is that if the parent was already cached, there is no need to additionally
              // cache all its children (unless they're used with a different parent somewhere else).
              val updatedCounts = currentCounts.map {
                case (op, count) => op -> (if (currentOp.containsTree(op)) count - currentOpCount else count)
              }
              updatedOps -> updatedCounts
            } else {
              currentOpsToCache -> currentCounts
            }
        }
      }
      opsToCache.map(op => op -> Cache(op)).toMap
    }

    private def identifyDuplicates(input: PhysicalOperator): Map[PhysicalOperator, Int] = {
      input
        .foldLeft(Map.empty[PhysicalOperator, Int].withDefaultValue(0)) {
          case (agg, op) => agg.updated(op, agg(op) + 1)
        }
        .filter(_._2 > 1)
    }
  }

}
