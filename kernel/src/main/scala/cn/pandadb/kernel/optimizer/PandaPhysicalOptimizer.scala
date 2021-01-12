//package cn.pandadb.kernel.optimizer
//
////import cn.pandadb.kernel.kv.AnyValue
//import org.opencypher.lynx.graph.LynxPropertyGraph
//import org.opencypher.lynx.plan.{Add, AddInto, Aggregate, Alias, Cache, ConstructGraph, Distinct, Drop, EmptyRecords, Filter, FromCatalogGraph, GraphUnionAll, Join, Limit, OrderBy, PhysicalOperator, PrefixGraph, ReturnGraph, Select, Skip, Start, SwitchContext, TabularUnionAll}
//import org.opencypher.lynx.planning.{LinTable, ScanNodes, ScanRels}
//import org.opencypher.lynx.{LynxPlannerContext, LynxTable, RecordHeader}
//import org.opencypher.okapi.ir.api.expr.{BinaryPredicate, ElementProperty, Equals, Expr, GreaterThan, GreaterThanOrEqual, Id, LessThan, LessThanOrEqual, NodeVar, Not, Param, RelationshipVar, Var}
//import org.opencypher.okapi.trees.TopDown
//
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//
//object PandaPhysicalOptimizer {
//
//  def process(input: PhysicalOperator)(implicit context: LynxPlannerContext): PhysicalOperator = {
//    //InsertCachingOperators(input)
//    //filterPushDown(input)
//    //val t1 = System.currentTimeMillis()
//    //val i = filterPushDown2(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), input)
//    //val t2 = System.currentTimeMillis()
//    //println(s"Optimizer cost time:${t2-t1}")
//    filterPushDown2(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), input)
//    //input
//
//  }
//
//  def filterPushDown2(filterOps: ArrayBuffer[PhysicalOperator], ordinaryOps: ArrayBuffer[PhysicalOperator], input: PhysicalOperator): PhysicalOperator = {
//    input match {
//      case x: Filter => {
//        if(x.expr.isInstanceOf[Not]){
//          filterPushDown2(filterOps, ordinaryOps, x.in)
//        }
//        else {
//          filterOps += x
//          filterPushDown2(filterOps, ordinaryOps, x.in)
//        }
//      }
//      /*    case x: LabelRecorders => {
//            filterOps += x
//            extractFilter(filterOps, ordinaryOps, x.in)
//          }*/
//      case x: Start =>
//        generatePhysicalPlan(filterOps, ordinaryOps, x)
//        //todo throw exception
//      case x: Select =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//
//      case x: Add =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: AddInto =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: Aggregate =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: Alias =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: Cache =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: ConstructGraph =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: Distinct =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: Drop[Any] =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: EmptyRecords =>
//        if (x.fields.size >= 1) filterOps += x
//        else ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//        //todo throw exception
//      case x: FromCatalogGraph =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: GraphUnionAll =>
//        generatePhysicalPlan(filterOps, ordinaryOps, x)
//      //todo throw exception
//      case x: Join =>
//        val op1 = filterPushDown(x.lhs)
//        val op2 = filterPushDown(x.rhs)
//        val join = Join(op1, op2, x.joinExprs, x.joinType)
//        generatePhysicalPlan(filterOps, ordinaryOps, join)
//      //todo throw exception
//      case x: Limit =>
//        filterOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: OrderBy =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: PrefixGraph =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: ReturnGraph =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: Skip =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: SwitchContext =>
//        ordinaryOps += x
//        filterPushDown2(filterOps, ordinaryOps, x.in)
//      case x: TabularUnionAll =>
//        //val op1 = extractFilter(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), x.lhs)
//        val op1 = filterPushDown(x.lhs)
//        //val op2 = extractFilter(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), x.rhs)
//        val op2 = filterPushDown(x.rhs)
//        val tabularUnionAll = TabularUnionAll(op1, op2)
//        generatePhysicalPlan(filterOps, ordinaryOps, tabularUnionAll)
//        //todo throw exception
//      case x: ScanRels => handlePhyOpWithStat(filterOps.map(Transformer.getPredicate(_)), ordinaryOps, Seq[(PhysicalOperator, Long)](), x)
//      case x: ScanNodes => handlePhyOpWithStat(filterOps.map(Transformer.getPredicate(_)), ordinaryOps, Seq[(PhysicalOperator, Long)](), x)
//    }
//  }
//
//  def handlePhyOpWithStat(filterOps: ArrayBuffer[NFPredicate], ordinaryOps: ArrayBuffer[PhysicalOperator], scanOps:Seq[(PhysicalOperator, Long)], input: PhysicalOperator): PhysicalOperator = {
//    input match {
//      case x:ScanNodes => {
//        //ScanNodes(isEnd: Boolean, nodeVar: Var, varMap: Map[Var, TNode], in: PhysicalOperator, next: PhysicalOperator, labels: Set[String], filterOP: ArrayBuffer[NFPredicate])
//        val xop = filterOps.filter(u => u.isInstanceOf[NFBinaryPredicate] && u.asInstanceOf[NFBinaryPredicate].getName().equals(x.nodeVar.name))
//        val newXop = ScanNodes(x.isEnd, x.nodeVar, x.varMap, x.in, x.next, x.labels, x.filterOP ++ xop)
//
//        if (newXop.isEnd){
//          generationPlan(newXop.asInstanceOf[ScanNodes].in, filterOps, ordinaryOps, scanOps ++ Seq(newXop -> newXop.getRecordsNumbers))
//        }
//        else handlePhyOpWithStat(filterOps, ordinaryOps, scanOps ++ Seq(newXop -> newXop.getRecordsNumbers), newXop.next)
//      }
//        //todo add graph for rels
//      case x:ScanRels =>{
//        //ScanRels(isEnd: Boolean,
//        //                           sVar: Var,
//        //                           rel: Var,
//        //                           tVar: Var,
//        //                           //scanType: ScanType,
//        //                           next: PhysicalOperator,
//        //                           direction: Direction, labels: Set[String],
//        //                           filterOP: ArrayBuffer[NFPredicate])
//        val xop = filterOps.filter(u => u.isInstanceOf[NFBinaryPredicate] && u.asInstanceOf[NFBinaryPredicate].getName().equals(x.rel.name))
//        val newXop = ScanRels(x.isEnd, x.sVar, x.rel, x.tVar, x.next, x.direction,  x.labels, x.filterOP ++ xop)
//        if (newXop.isEnd){
//          generationPlan(newXop.asInstanceOf[ScanNodes].in, filterOps, ordinaryOps, scanOps ++ Seq(newXop -> newXop.getRecordsNumbers))
//        }
//        else handlePhyOpWithStat(filterOps, ordinaryOps, scanOps ++ Seq(newXop -> newXop.getRecordsNumbers), newXop.next)
//      }
//    }
//  }
//
//  def generationPlan(in: PhysicalOperator, filterOps: ArrayBuffer[NFPredicate], ordinaryOps: ArrayBuffer[PhysicalOperator], scanOps:Seq[(PhysicalOperator, Long)]): PhysicalOperator ={
//    val lop = filterOps.filter(_.isInstanceOf[NFLimit])
//    val limit = if(lop.nonEmpty) lop.head else null
//    //var opWithCnt: mutable.Map[PhysicalOperator, Long] = mutable.Map[PhysicalOperator, Long]()
//
//    val (op, cnt) = scanOps.toArray.minBy(_._2)
//
//    //val index = scanOps.values.toArray.zipWithIndex.filter(_._1 == cnt).head._2
//    val index = scanOps.zipWithIndex.filter(_._1._2 ==cnt).head._2
//
//    val leftOPs = scanOps.take(index).toArray.reverse.toBuffer
//    val rigthOps = scanOps.takeRight(scanOps.size - 1 - index).toArray.toBuffer
//
//    val rets = ordinaryOps.head.asInstanceOf[Select].expressions.map(v =>
//      v match {
//        case x:NodeVar => {
//          if (x.name.contains(".")) x.name.take(x.name.indexOf("."))
//          else x.name
//        }
//        case x:RelationshipVar => {
//          if (x.name.contains(".")) x.name.take(x.name.indexOf("."))
//          else x.name
//        }
//      }).toSet
//
//    var tempOp: PhysicalOperator = op match {
//      case x: ScanRels => ScanRels(true, x.sVar, x.rel, x.tVar, in , x.direction,  x.labels, x.filterOP, rets.contains(x.rel.name), false)
//      case x: ScanNodes => ScanNodes(true, x.nodeVar, x.varMap, x.in, null, x.labels, x.filterOP, rets.contains(x.nodeVar.name), false)
//    }
//    while (leftOPs.nonEmpty && rigthOps.nonEmpty){
//      if (leftOPs.head._2 >= rigthOps.head._2){
//        rigthOps.head._1 match {
//          case x: ScanNodes => tempOp = ScanNodes(false, x.nodeVar, x.varMap, x.in, tempOp, x.labels, x.filterOP, rets.contains(x.nodeVar.name), false)
//          case x: ScanRels => tempOp = ScanRels(false, x.sVar, x.rel, x.tVar, tempOp, x.direction,  x.labels, x.filterOP, rets.contains(x.rel.name), false)
//        }
//        rigthOps.remove(0)
//      }
//      else{
//        leftOPs.head._1 match {
//          case x: ScanNodes => tempOp = ScanNodes(false, x.nodeVar, x.varMap, x.in, tempOp, x.labels, x.filterOP, rets.contains(x.nodeVar.name), false)
//          case x: ScanRels => tempOp = ScanRels(false, x.sVar, x.rel, x.tVar, tempOp, x.direction,  x.labels, x.filterOP, rets.contains(x.rel.name), false)
//        }
//        leftOPs.remove(0)
//      }
//    }
//    while(leftOPs.nonEmpty){
//      leftOPs.head._1 match {
//        case x: ScanNodes => tempOp = ScanNodes(false, x.nodeVar, x.varMap, x.in, tempOp, x.labels, x.filterOP, rets.contains(x.nodeVar.name), false)
//        case x: ScanRels => tempOp = ScanRels(false, x.sVar, x.rel, x.tVar, tempOp, x.direction,  x.labels, x.filterOP, rets.contains(x.rel.name), false)
//      }
//      leftOPs.remove(0)
//    }
//    while(rigthOps.nonEmpty){
//      rigthOps.head._1 match {
//        case x: ScanNodes => tempOp = ScanNodes(false, x.nodeVar, x.varMap, x.in, tempOp, x.labels, x.filterOP, rets.contains(x.nodeVar.name), false)
//        case x: ScanRels => tempOp = ScanRels(false, x.sVar, x.rel, x.tVar, tempOp, x.direction,  x.labels, x.filterOP, rets.contains(x.rel.name), false)
//      }
//      rigthOps.remove(0)
//    }
//
//    if (limit != null){
//      tempOp match {
//        case x: ScanNodes => tempOp = ScanNodes(x.isEnd, x.nodeVar, x.varMap, x.in, x.next, x.labels, x.filterOP += limit, x.isReturn, x.isTable)
//        case x: ScanRels => tempOp = ScanRels(x.isEnd, x.sVar, x.rel, x.tVar, x.next, x.direction,  x.labels, x.filterOP += limit, x.isReturn, x.isTable)
//      }
//    }
//
//    //
//
//    tempOp =  tempOp match {
//      case x: ScanNodes => ScanNodes(x.isEnd, x.nodeVar, x.varMap, x.in, x.next, x.labels, x.filterOP , x.isReturn)
//      case x: ScanRels => ScanRels(x.isEnd, x.sVar, x.rel, x.tVar, x.next, x.direction,  x.labels, x.filterOP, x.isReturn)
//    }
//
//    //ordinaryOps.reverse
//    //var tempOp: PhysicalOperator = Transformer(filterops, endOp, reOrderPredicates(filterops, endOp.graph))
//    ordinaryOps.reverse.foreach(u => {
//      tempOp = constructPhysicalPlan(u, tempOp)
//    })
//    tempOp
//
//  }
//
///*  def getTable(in: PhysicalOperator, table: LinTable):LinTable = {
//    in match {
//      case x:ScanNodes =>{
//        if(x.isEnd)
//      }
//      case x:ScanRels =>
//    }
//  }*/
//
//  def filterPushDown(input: PhysicalOperator): PhysicalOperator = {
//    val newPlan: PhysicalOperator = extractFilter(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), input)
//    if(newPlan==null) input
//    else newPlan
//  }
//
//  def extractFilter(filterOps: ArrayBuffer[PhysicalOperator], ordinaryOps: ArrayBuffer[PhysicalOperator], input: PhysicalOperator): PhysicalOperator = {
//
//    input match {
//      case x: Filter => {
//        filterOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      }
//  /*    case x: LabelRecorders => {
//        filterOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      }*/
//      case x: Start =>
//        generatePhysicalPlan(filterOps, ordinaryOps, x)
//      case x: Select =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//
//      case x: Add =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: AddInto =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: Aggregate =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: Alias =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: Cache =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: ConstructGraph =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: Distinct =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: Drop[Any] =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: EmptyRecords =>
//        if (x.fields.size >= 1) filterOps += x
//        else ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: FromCatalogGraph =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: GraphUnionAll =>
//        generatePhysicalPlan(filterOps, ordinaryOps, x)
//      case x: Join =>
//        val op1 = filterPushDown(x.lhs)
//        val op2 = filterPushDown(x.rhs)
//        val join = Join(op1, op2, x.joinExprs, x.joinType)
//        generatePhysicalPlan(filterOps, ordinaryOps, join)
//      case x: Limit =>
//        filterOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: OrderBy =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: PrefixGraph =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: ReturnGraph =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: Skip =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: SwitchContext =>
//        ordinaryOps += x
//        extractFilter(filterOps, ordinaryOps, x.in)
//      case x: TabularUnionAll =>
//        //val op1 = extractFilter(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), x.lhs)
//        val op1 = filterPushDown(x.lhs)
//        //val op2 = extractFilter(new ArrayBuffer[PhysicalOperator](), new ArrayBuffer[PhysicalOperator](), x.rhs)
//        val op2 = filterPushDown(x.rhs)
//        val tabularUnionAll = TabularUnionAll(op1, op2)
//        generatePhysicalPlan(filterOps, ordinaryOps, tabularUnionAll)
//     // case x: ScanNodes =>
//      //case x: ScanRels =>
//    }
//  }
//
//  def isLabel(in: PhysicalOperator): Boolean = {
//    in match {
//      case x:EmptyRecords => true
//      case _ => false
//    }
//  }
//
//  def isNecessaryPPD(filterops: ArrayBuffer[PhysicalOperator], graph:LynxPropertyGraph): Boolean = {
//    //val prediates = ArrayBuffer[NFPredicate]()
//    //filterops.foreach(u =>{
//    //  prediates += PpdFilter.getPredicate(u)
//    filterops
//   // })
//    //todo figure out whether PPD is necessary
//    val prediates = ArrayBuffer[NFPredicate]()
//    filterops.foreach(u =>{
//      prediates += Transformer.getPredicate(u)
//    })
//
//    filterops.map(isLabel(_)).reduce(_|_) //&& graph.asInstanceOf[PandaPropertyGraph[Id]].isNFPredicatesWithIndex(prediates.toArray)
//
//    //graph.asInstanceOf[PandaPropertyGraph].isNFPredicatesWithIndex(prediates.toArray)
//  }
//
//  def reOrderPredicates(filterops: ArrayBuffer[PhysicalOperator], graph:LynxPropertyGraph): Array[NFPredicate] = {
//    //todo reorder pddfilter according to graph node counts
//    val prediates = ArrayBuffer[NFPredicate]()
//    filterops.foreach(u =>{
//      prediates += Transformer.getPredicate(u)
//     })
//    prediates.toArray
//  }
//
//
//
//  def generatePhysicalPlan(filterops: ArrayBuffer[PhysicalOperator], opseq: ArrayBuffer[PhysicalOperator], endOp: PhysicalOperator): PhysicalOperator = {
//    if (!filterops.isEmpty) {
//      if(isNecessaryPPD(filterops, endOp.graph)) {
//        var tempOp: PhysicalOperator = Transformer(filterops, endOp, reOrderPredicates(filterops, endOp.graph))
//        opseq.reverse.foreach(u => {
//          tempOp = constructPhysicalPlan(u, tempOp)
//        })
//        tempOp
//      }
//      else null
//    }
//    else null
//
//  }
//
//  def constructPhysicalPlan(current: PhysicalOperator, in: PhysicalOperator): PhysicalOperator = {
//    //todo with null
//    current match {
//      case x: Filter => null
//      //case x: LabelRecorders => null
//      case x: Start => null
//      case x: Select => Select(in, x.expressions, x.columnRenames)
//      case x: Add => Add(in, x.exprs)
//      case x: AddInto => AddInto(in, x.valueIntoTuples)
//      case x: Aggregate => Aggregate(in, x.group, x.aggregations)
//      case x: Alias => Alias(in, x.aliases)
//      case x: Cache => Cache(in)
//      case x: ConstructGraph => ConstructGraph(in, x.constructedGraph, x.construct, x.context)
//      case x: Distinct => Distinct(in, x.fields)
//      case x: Drop[Expr] => Drop(in, x.exprs)
//      case x: EmptyRecords => EmptyRecords(in, x.fields)
//      case x: FromCatalogGraph => FromCatalogGraph(in, x.logicalGraph)
//      case x: GraphUnionAll => null
//      case x: Join => null
//      case x: Limit => Limit(in, x.expr)
//      case x: OrderBy => OrderBy(in, x.sortItems)
//      case x: PrefixGraph => PrefixGraph(in, x.prefix)
//      case x: ReturnGraph => ReturnGraph(in)
//      case x: Skip => Skip(in, x.expr)
//      case x: SwitchContext => {
//        in match {
//          case y:Select => y.in
//          case _ => SwitchContext(in, x.context)
//        }
//      }
//      case x: TabularUnionAll => null
//    }
//
//  }
//  object InsertCachingOperators {
//
//    def apply(input: PhysicalOperator): PhysicalOperator = {
//      val replacements = calculateReplacementMap(input).filterKeys {
//        case _: Start => false
//        case _ => true
//      }
//
//      val nodesToReplace = replacements.keySet
//
//      TopDown[PhysicalOperator] {
//        case cache: Cache => cache
//        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
//          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
//          parent.withNewChildren(newChildren)
//      }.transform(input)
//    }
//
//    private def calculateReplacementMap(input: PhysicalOperator): Map[PhysicalOperator, PhysicalOperator] = {
//      val opCounts = identifyDuplicates(input)
//      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
//      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[PhysicalOperator] -> opCounts) { (agg, currentOp) =>
//        agg match {
//          case (currentOpsToCache, currentCounts) =>
//            val currentOpCount = currentCounts(currentOp)
//            if (currentOpCount > 1) {
//              val updatedOps = currentOpsToCache + currentOp
//              // We're traversing `opsByHeight` from largest to smallest query sub-tree.
//              // We pick the trees with the largest height for caching first, and then reduce the duplicate count
//              // for the sub-trees of the cached tree by the number of times the parent tree appears.
//              // The idea behind this is that if the parent was already cached, there is no need to additionally
//              // cache all its children (unless they're used with a different parent somewhere else).
//              val updatedCounts = currentCounts.map {
//                case (op, count) => op -> (if (currentOp.containsTree(op)) count - currentOpCount else count)
//              }
//              updatedOps -> updatedCounts
//            } else {
//              currentOpsToCache -> currentCounts
//            }
//        }
//      }
//      opsToCache.map(op => op -> Cache(op)).toMap
//    }
//
//    private def identifyDuplicates(input: PhysicalOperator): Map[PhysicalOperator, Int] = {
//      input
//        .foldLeft(Map.empty[PhysicalOperator, Int].withDefaultValue(0)) {
//          case (agg, op) => agg.updated(op, agg(op) + 1)
//        }
//        .filter(_._2 > 1)
//    }
//  }
//
//}
