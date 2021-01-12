//package cn.pandadb.kernel.optimizer
//
//import cn.pandadb.kernel.optimizer.NFEquals
//import cn.pandadb.kernel.optimizer.LynxType.{LynxNode, LynxRelationship}
//import cn.pandadb.kernel.store.{StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
//import org.opencypher.lynx.{LynxPlannerContext, LynxRecords, LynxSession, LynxTable, PropertyGraphScanner, RecordHeader}
//import org.opencypher.lynx.graph.{LynxPropertyGraph, ScanGraph, WritableScanGraph}
//import org.opencypher.lynx.ir.{IRNode, IRRelation, PropertyGraphWriter, WritablePropertyGraph}
//import org.opencypher.lynx.plan.Filter
//import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
//import org.opencypher.okapi.api.value.CypherValue
//import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}
//import org.opencypher.okapi.ir.api.expr.{EndNode, NodeVar, RelationshipVar, StartNode}
//import org.parboiled.scala.utils.Predicate
//
//import scala.collection.mutable.ArrayBuffer
//
//
//
//class PandaPropertyGraph[Id](scan: PandaPropertyGraphScan[Id], writer: PropertyGraphWriter[Id])(implicit override val session: LynxSession)
//  //extends ScanGraph[Id](scan)(session)
//  extends WritableScanGraph[Id](scan, writer)(session){
//  //def getRecorderNumberFromPredicate(predicate: NFPredicate): Int = ???
//
//  def isNFPredicateWithIndex(predicate: NFPredicate, labels: Set[String]): Boolean = {
//
//  /*  predicate match{
//      case x:NFEquals => scan.isPropertyWithIndex(labels, x.propName)
//      case x:NFGreaterThan => scan.isPropertyWithIndex(labels, x.propName)
//      case x:NFGreaterThanOrEqual => scan.isPropertyWithIndex(labels, x.propName)
//      case x:NFLessThan => scan.isPropertyWithIndex(labels, x.propName)
//      case x:NFLessThanOrEqual => scan.isPropertyWithIndex(labels, x.propName)
//    }*/
//    true
//
//  }
//
//  //def
//
//
//  def isNFPredicatesWithIndex(predicate: Array[NFPredicate]): Boolean = {
//    //predicate.map()
//    val (predicateNew, labels) = findLabelPredicate(predicate)
//
//    if (predicateNew.nonEmpty) predicateNew.map(isNFPredicateWithIndex(_, labels.distinct.toSet)).reduce(_|_)
//    else true
//  }
//
//  def findLabelPredicate(predicate: Array[NFPredicate]): (Array[NFPredicate], Seq[String]) = {
//    var nps: ArrayBuffer[NFPredicate] = ArrayBuffer[NFPredicate]()
//    var labels: Seq[String] = Seq[String]()
//    predicate.foreach(u => {
//      u match {
//        case x: NFLabels => {
//          labels ++= x.labels
//        }
//        case x: NFPredicate => nps += x
//      }
//    })
//    nps.toArray -> labels
//  }
//
//  def findLimitPredicate(predicate: Array[NFPredicate]):(ArrayBuffer[NFPredicate], Long) = {
//    var nps: ArrayBuffer[NFPredicate] = ArrayBuffer[NFPredicate]()
//    var limit: Long = -1
//    predicate.foreach(u => {
//      u match {
//        case x: NFLimit => {
//          limit = x.size
//        }
//        case x: NFPredicate => nps += x
//      }
//    })
//    nps -> limit
//  }
//  def findindexPredicate(predicate: Array[NFPredicate], labels: Set[String]):(Array[NFPredicate], Array[NFPredicate]) = {
//  /*  var npsWithIndex: ArrayBuffer[NFPredicate] = ArrayBuffer[NFPredicate]()
//    var nps: ArrayBuffer[NFPredicate] = ArrayBuffer[NFPredicate]()
//    predicate.foreach(u => {
//      if
//    })*/
//    predicate.filter(isNFPredicateWithIndex(_, labels)) -> predicate.filter(!isNFPredicateWithIndex(_, labels))
//  }
//
//
//
//
//  /*  def findFirstPredicate(predicate: Array[NFPredicate]): ( Array[NFPredicate], NFPredicate) = {
//
//    }*/
//
//
//
//
//  def filterByPredicates(node: Node[Id], predicate: Array[NFPredicate]): Boolean = {
//    if (predicate.nonEmpty) predicate.map(isOkNode(_, node)).reduce(_&&_)
//    else true
//  }
//
//  def getNodeCnt(predicate: Array[NFPredicate], labels: Set[String]): Long = {
//    if(labels.isEmpty) scan.getAllNodesCount()
//    else{
//      labels.map(scan.getNodesCountByLabel).min
//    }
//  }
//
//  def getRelCnt(predicate: Array[NFPredicate], label: String, direction: Int): Long = {
//    if (label ==null) scan.getAllRelsCount()
//    else scan.getRelsCountByLabel(label)
//  }
//
///*  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String]): Iterable[Node[Id]] ={
//    val node1 = LynxNode(1,Set("person"), "name" -> CypherValue("bob"), "age" -> CypherValue(40))
//    val node2 = LynxNode(1,Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(20))
//    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
//    val nodes:Map[Long, LynxNode] = Map(1L->node1, 2L -> node2, 3L->node3)
//    nodes.values.map(_.asInstanceOf[Node[Id]]).filter(filterByPredicates(_, predicate))
//  }
//
//  def getNodesByFilter(prediates: Array[NFPredicate], name: String, nodeCypherType: CTNode): LynxRecords = {
//    val node1 = LynxNode(1,Set("person"), "name" -> CypherValue("bob"), "age" -> CypherValue(40))
//    val node2 = LynxNode(1,Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(20))
//    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
//    val nodes:Map[Long, LynxNode] = Map(1L->node1, 2L -> node2, 3L->node3)
//    nodes.values.map(_.asInstanceOf[Node[Id]]).filter(filterByPredicates(_, prediates))
//    new LynxRecords(null, null)
//  }
//  def getNodesByFilter(prediates: Array[NFPredicate],labels: Set[String], varNode: NodeVar): LynxRecords = {
//    val node1 = LynxNode(1,Set("person"), "name" -> CypherValue("bob"), "age" -> CypherValue(40))
//    val node2 = LynxNode(1,Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(20))
//    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
//    val nodes:Map[Long, LynxNode] = Map(1L->node1, 2L -> node2, 3L->node3)
//    nodes.values.map(_.asInstanceOf[Node[Id]]).filter(filterByPredicates(_, prediates))
//    new LynxRecords(null, null)
//  }*/
//
//
////  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String], sk: Int): Iterable[Node[Id]] = {
////
////
////    //todo test
////    val nodes = {
////      if (labels.nonEmpty) {
////        if (predicate.nonEmpty) {
////          val (indexNfp, nfp) = findindexPredicate(predicate, labels)
////          val tempnodes = {
////            if (indexNfp.nonEmpty) indexNfp.map(scan.allNodes(_, labels).toSeq).reduce(_.intersect(_))
////            else scan.allNodes(labels, false)
////          }
////          tempnodes.filter(filterByPredicates(_, nfp))
////        }
////        else scan.allNodes(labels, false)
////      }
////      else scan.allNodes()
////    }
////    nodes
////  }
////
////  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String], nodeVar: NodeVar): LynxRecords = {
////
////    new LynxRecords(
////      RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name)),
////      LynxTable(Seq(nodeVar.name -> CTNode), getNodesByFilter(predicate, labels).map(Seq(_)))
////    )
////
////  }
//
//
//
//
//
//
//
//
//
//
//
//  def isRangePredicate(p: NFPredicate): Boolean = {
//    if (p.isInstanceOf[NFGreaterThan] || p.isInstanceOf[NFLessThan] || p.isInstanceOf[NFGreaterThanOrEqual] || p.isInstanceOf[NFLessThanOrEqual]) true
//    else false
//  }
//  def UnionFilter(Ops: ArrayBuffer[NFPredicate]): ArrayBuffer[NFPredicate] = {
//    var newOps: ArrayBuffer[NFPredicate] = new ArrayBuffer[NFPredicate]()
//    if (Ops.isEmpty) new ArrayBuffer[NFPredicate]()
//    else {
//      Ops.filter(isRangePredicate(_)).groupBy(_.asInstanceOf[NFBinaryPredicate].getName())
//    }
//    null
//  }
//
//
//  //******************************************************************************************
//  //********************************nodes*****************************************************
//  //******************************************************************************************
//
//
//  def isOkNode(p: NFPredicate, node: Node[Id]): Boolean = {
//    p match {
//      case x:NFGreaterThanOrEqual => x.isInRange(node.properties.get(x.propName).get.getValue.get)
//      case x:NFLessThanOrEqual => x.isInRange(node.properties.get(x.propName).get.getValue.get)
//      case x:NFEquals => {
//        if(node.properties.get(x.propName).get.getValue.get.equals(x.value.anyValue) ) {
//          true
//        }
//        else {
//          false
//        }
//      }
//      case x:NFLessThan => x.isInRange(node.properties.get(x.propName).get.getValue.get)
//      case x:NFGreaterThan => x.isInRange(node.properties.get(x.propName).get.getValue.get)
//    }
//  }
//
//  def isOkNode(p: NFPredicate, node:StoredNodeWithProperty): Boolean = {
//    p match {
//      case x:NFGreaterThanOrEqual => x.isInRange(node.properties.get(scan.getNodePropertyIdByName(x.propName)))
//      case x:NFLessThanOrEqual => x.isInRange(node.properties.get(scan.getNodePropertyIdByName(x.propName)))
//      case x:NFEquals => {
//        val pid = scan.getNodePropertyIdByName(x.propName)
//        val v = node.properties(pid)
//        if(v.equals(x.value.anyValue) ) {
//          true
//        }
//        else {
//          false
//        }
//      }
//      case x:NFLessThan => x.isInRange(node.properties.get(scan.getNodePropertyIdByName(x.propName)))
//      case x:NFGreaterThan => x.isInRange(node.properties.get(scan.getNodePropertyIdByName(x.propName)))
//    }
//  }
//
//
//  def getNodeById(nodeId: Long, labels: Set[String], filterOP: ArrayBuffer[NFPredicate]): Option[Node[Id]] ={
//    val node = scan.mapNode(scan.getNodeById(nodeId))
//    if(labels.isEmpty){
//      if(filterOP.isEmpty) Some(node)
//      else {
//        if(filterOP.map(isOkNode(_, node)).reduce(_ && _)) Some(node)
//        else None
//      }
//    }
//    else{
//      if (!labels.map(node.labels.contains(_)).reduce(_ && _)) None
//      else{
//        if(filterOP.isEmpty) Some(node)
//        else {
//          if(filterOP.map(isOkNode(_, node)).reduce(_ && _)) Some(node)
//          else None
//        }
//      }
//    }
//  }
//
//  def isOkNodesId(id: Long, labels: Set[String], filterOP: ArrayBuffer[NFPredicate]): Boolean = {
//    if (labels.isEmpty) {
//      if (filterOP.isEmpty) true
//      else {
//        filterOP.map(isOkNode(_, scan.getNodeById(id))).reduce(_ && _)
//      }
//    }
//    else {
//      if (filterOP.isEmpty) scan.hasNodeLabels(id, labels)
//      else scan.hasNodeLabels(id, labels) && filterOP.map(isOkNode(_, scan.getNodeById(id))).reduce(_ && _)
//    }
//  }
//
//  def getNodeById(id: Long, isReturn: Boolean): StoredNode = {
//    if (isReturn) scan.getNodeById(id)
//    else StoredNode(id)
//  }
//
//  def getNodeById(nodeId: Long, labels: Set[String], filterOP: ArrayBuffer[NFPredicate], isReturn: Boolean): Option[StoredNode] ={
//    //todo if else
//    val node = scan.getNodeById(nodeId)
//    if(labels.isEmpty){
//      if(filterOP.isEmpty) Some(node)
//      else {
//        if(filterOP.map(isOkNode(_, node)).reduce(_ && _)) Some(node)
//        else None
//      }
//    }
//    else{
//      if (!labels.map(scan.getNodeLabelsNameByIds(node.labelIds).contains(_)).reduce(_ && _)) None
//      else{
//        if(filterOP.isEmpty) Some(node)
//        else {
//          if(filterOP.map(isOkNode(_, node)).reduce(_ && _)) Some(node)
//          else None
//        }
//      }
//    }
//  }
//
//  def filterNode(node: Node[Id], ops: ArrayBuffer[NFPredicate]): Boolean = {
//    if (ops.isEmpty) true
//    else {
//      ops.map(isOkNode(_, node)).reduce(_ && _)
//    }
//  }
//
//  def filterNode(node: StoredNodeWithProperty, ops: ArrayBuffer[NFPredicate]): Boolean = {
//    if (ops.isEmpty) true
//    else {
//      ops.map(isOkNode(_, node)).reduce(_ && _)
//    }
//  }
//
//  def filterNode(node: Node[Id], labels: Set[String]): Boolean = {
//    if (labels.isEmpty) true
//    else {
//      labels.map(node.labels.contains(_)).reduce(_ && _)
//    }
//  }
//
//  def filterNode(node: StoredNodeWithProperty, labels: Set[String]): Boolean = {
//    if (labels.isEmpty) true
//    else {
//      labels.map(scan.getNodeLabelsNameByIds(node.labelIds).contains(_)).reduce(_ && _)
//    }
//  }
//
//  def getPropNames(ops: ArrayBuffer[NFPredicate]): Set[String] = {
//    ops.map(_.asInstanceOf[NFBinaryPredicate].getPropName()).toSet
//  }
//
//  def getNodesByFilter(predicate: Array[NFPredicate], name: String, nodeCypherType: CTNode): LynxRecords = {
//
//
//    val (predicateNew1, labels) = findLabelPredicate(predicate)
//    new LynxRecords(RecordHeader(Map(NodeVar(name)(CTNode) -> name)),
//      LynxTable(Seq(name -> CTNode), getNodesByFilter(predicateNew1, labels.toSet).toIterable.map(Seq(_)))
//    )
//  }
//
//  def findIndexIdAndValue(ops: ArrayBuffer[NFPredicate], labels: Set[String]): (Int, Any) = ???
//
//  def getNodesByFilter(labels: Set[String]): Iterator[Node[Id]] = {
//    if (labels.size ==1) scan.getNodesByLabel(labels.head).map(scan.mapNode)
//    else {
//      val label = labels.toArray.map(x => x -> scan.getNodesCountByLabel(x)).minBy(_._2)._1
//      scan.getNodesByLabel(label).map(scan.mapNode).filter(filterNode(_, labels -- Set(label)))
//    }
//  }
//
//  def getNodesByFilter(labels: Set[String], isReturn:Boolean): Iterator[StoredNodeWithProperty] = {
//    if (labels.size ==1) scan.getNodesByLabel(labels.head)
//    else {
//      val label = labels.toArray.map(x => x -> scan.getNodesCountByLabel(x)).minBy(_._2)._1
//      scan.getNodesByLabel(label).filter(filterNode(_, labels -- Set(label)))
//    }
//  }
//
//  def getNodesByFilter(predicate: Array[NFPredicate],labels: Set[String], nodeVar: NodeVar): LynxRecords = {
//
//    new LynxRecords(RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name)),
//      LynxTable(Seq(nodeVar.name -> CTNode), getNodesByFilter(predicate, labels).toIterable.map(Seq(_)))
//        )
//  }
//
//  def getNodesByFilter(ops: Array[NFPredicate], labels: Set[String]): Iterator[Node[Id]] = {
//    if(labels.isEmpty){
//      if(ops.isEmpty)
//        scan.getAllNodes().map(scan.mapNode)
//      else{
//        val (opsNew, size) = findLimitPredicate(ops.toArray)
//        if (size > 0 )
//          scan.getAllNodes().map(scan.mapNode).filter(filterNode(_, opsNew)).take(size.toInt)
//        else
//          scan.getAllNodes().map(scan.mapNode).filter(filterNode(_, opsNew))
//      }
//    }
//    else {
//      if (ops.isEmpty){
//        getNodesByFilter(labels)
//      }
//      else{
//        val (opsNew, size) = findLimitPredicate(ops.toArray)
//        val eqlops = opsNew.filter(_.isInstanceOf[NFEquals]).map(_.asInstanceOf[NFEquals]).map(x => x.propName -> x.value.anyValue)
//        val rangeops = opsNew.filter(!_.isInstanceOf[NFEquals]).map(_.asInstanceOf[NFBinaryPredicate]).map(x => {
//          x.getPropName()->(x.getValue().anyValue,x.getType())
//        })
//        if(eqlops.isEmpty&&rangeops.isEmpty){
//          if (size > 0) getNodesByFilter(labels).take(size.toInt)
//          else getNodesByFilter(labels)
//        }
//        else {
//          if (eqlops.isEmpty){
//            val (indexId,label,props, cnt) = scan.isPropertysWithIndex(labels, rangeops.map(_._1).toSet)
//            if (indexId >= 0) {
//              var (v,t) = props.toSeq.map(rangeops.toMap.get(_)).head.get
//              val max = Double.MaxValue
//              val min = Double.MinValue
//              val value = v match {
//                case v:Long => v.toDouble
//                case v:Double => v
//                case _ => 0
//              }
//
//              val nodes = t match {
//                case "<=" => scan.findRangeNode(indexId, min, value, toClose = true)
//                case "<" => scan.findRangeNode(indexId, min,  value)
//                case ">=" => scan.findRangeNode(indexId,  value, max, fromClose = true)
//                case ">" => scan.findRangeNode(indexId,  value, max)
//              }
//              if (size>0) nodes.map(scan.mapNode).filter(filterNode(_, opsNew)).take(size.toInt)
//              else nodes.map(scan.mapNode).filter(filterNode(_, opsNew))
//            }
//            else {
//              if (size>0) getNodesByFilter(labels).filter(filterNode(_, opsNew)).take(size.toInt)
//              else getNodesByFilter(labels).filter(filterNode(_, opsNew))
//            }
//          }
//          else{
//            val (indexId,label, props, cnt) = scan.isPropertysWithIndex(labels, eqlops.map(_._1).toSet)
//            if (indexId >= 0) {
//              val v = props.toSeq.map(eqlops.toMap.get(_)).head
//              if (size>0) scan.findNode(indexId, v.get).map(scan.mapNode).filter(filterNode(_, opsNew)).take(size.toInt)
//              else scan.findNode(indexId, v.get).map(scan.mapNode).filter(filterNode(_, opsNew))
//            }
//            else{
//              if (size>0) getNodesByFilter(labels).filter(filterNode(_, opsNew)).take(size.toInt)
//              else getNodesByFilter(labels).filter(filterNode(_, opsNew))
//            }
//
//          }
//        }
//      }
//    }
//  }
//
//
//  def getNodesByFilter(ops: Array[NFPredicate], labels: Set[String], isReturn: Boolean): Iterator[StoredNode] = {
//    if(isReturn) getNodesByFilter2(ops, labels, isReturn)
//    else getNodesByFilter2(ops, labels, isReturn).map(node => StoredNode(node.id))
//  }
//
//  def getNodesByFilter2(ops: Array[NFPredicate], labels: Set[String], isReturn: Boolean): Iterator[StoredNode] = {
//    if(labels.isEmpty){
//      if(ops.isEmpty)
//        scan.getAllNodes()
//      else{
//        val (opsNew, size) = findLimitPredicate(ops.toArray)
//        if (size > 0 )
//          scan.getAllNodes().filter(filterNode(_, opsNew)).take(size.toInt)
//        else
//          scan.getAllNodes().filter(filterNode(_, opsNew))
//      }
//    }
//    else {
//      if (ops.isEmpty){
//        getNodesByFilter(labels, isReturn)
//      }
//      else{
//        val (opsNew, size) = findLimitPredicate(ops.toArray)
//        val eqlops = opsNew.filter(_.isInstanceOf[NFEquals]).map(_.asInstanceOf[NFEquals]).map(x => x.propName -> x.value.anyValue)
//        val rangeops = opsNew.filter(!_.isInstanceOf[NFEquals]).map(_.asInstanceOf[NFBinaryPredicate]).map(x => {
//          x.getPropName()->(x.getValue().anyValue,x.getType())
//        })
//        if(eqlops.isEmpty&&rangeops.isEmpty){
//          if (size > 0) getNodesByFilter(labels,isReturn).take(size.toInt)
//          else getNodesByFilter(labels,isReturn)
//        }
//        else {
//          if (eqlops.isEmpty){
//            val (indexId,label,props, cnt) = scan.isPropertysWithIndex(labels, rangeops.map(_._1).toSet)
//            if (indexId >= 0) {
//              var (v,t) = props.toSeq.map(rangeops.toMap.get(_)).head.get
//              val max = Float.MaxValue
//              val min = Float.MinValue
//              val value = v match {
//                case v:Long => v.toInt.toFloat
//                case v:Double => v.toFloat
//              }
//
//              val nodes = t match {
//                case "<=" => scan.findRangeNode(indexId, min, value, toClose = true)
//                case "<" => scan.findRangeNode(indexId, min,  value)
//                case ">=" => scan.findRangeNode(indexId,  value, max, fromClose = true)
//                case ">" => scan.findRangeNode(indexId,  value, max)
//              }
//              if (size>0) nodes.filter(filterNode(_, opsNew)).take(size.toInt)
//              else nodes.filter(filterNode(_, opsNew))
//            }
//            else {
//              if (size>0) getNodesByFilter(labels, isReturn).filter(filterNode(_, opsNew)).take(size.toInt)
//              else getNodesByFilter(labels, isReturn).filter(filterNode(_, opsNew))
//            }
//          }
//          else{
//            val (indexId,label, props, cnt) = scan.isPropertysWithIndex(labels, eqlops.map(_._1).toSet)
//            if (indexId >= 0) {
//              val v = props.toSeq.map(eqlops.toMap.get(_)).head
//              if (size>0) scan.findNode(indexId, v.get).filter(filterNode(_, opsNew)).take(size.toInt)
//              else scan.findNode(indexId, v.get).filter(filterNode(_, opsNew))
//            }
//            else{
//              if (size>0) getNodesByFilter(labels, isReturn).filter(filterNode(_, opsNew)).take(size.toInt)
//              else getNodesByFilter(labels, isReturn).filter(filterNode(_, opsNew))
//            }
//
//          }
//        }
//      }
//    }
//  }
//
//
//
//
//
//
//
//
//
//  //******************************************************************************************
//  //********************************rels*****************************************************
//  //******************************************************************************************
//
//  def isOkRel(p: NFPredicate, rel: Relationship[Id]): Boolean = {
//    p match {
//      case x:NFGreaterThanOrEqual => if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] >= x.value.asInstanceOf[Int]) true else false
//      case x:NFLessThanOrEqual => if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] <= x.value.asInstanceOf[Int]) true else false
//      case x:NFEquals => {
//        if(rel.properties.get(x.propName).get.getValue.get.equals(x.value.anyValue) ) {
//          true
//        }
//        else {
//          false
//        }
//      }
//      case x:NFLessThan => if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] < x.value.asInstanceOf[Int]) true else false
//      case x:NFGreaterThan => if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] > x.value.asInstanceOf[Int]) true else false
//    }
//  }
//
//  def isOkRel(p: NFPredicate, rel: StoredRelationWithProperty): Boolean = {
//    p match {
//      case x:NFGreaterThanOrEqual => x.isInRange(rel.properties.get(scan.getRelationPropertyIdByName(x.propName)))//if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] >= x.value.asInstanceOf[Int]) true else false
//      case x:NFLessThanOrEqual => x.isInRange(rel.properties.get(scan.getRelationPropertyIdByName(x.propName)))//if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] <= x.value.asInstanceOf[Int]) true else false
//      case x:NFEquals => {
//        if(rel.properties.get(scan.getRelationPropertyIdByName(x.propName)).equals(x.value.anyValue) ) {
//          true
//        }
//        else {
//          false
//        }
//      }
//      case x:NFLessThan => x.isInRange(rel.properties.get(scan.getRelationPropertyIdByName(x.propName)))//if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] < x.value.asInstanceOf[Int]) true else false
//      case x:NFGreaterThan => x.isInRange(rel.properties.get(scan.getRelationPropertyIdByName(x.propName)))//if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] > x.value.asInstanceOf[Int]) true else false
//    }
//  }
//
//  def filterRel(rel: Relationship[Id], ops: ArrayBuffer[NFPredicate]): Boolean = {
//    if (ops.isEmpty) true
//    else {
//      ops.map(isOkRel(_, rel)).reduce(_ && _)
//    }
//  }
//  def filterRel(rel: StoredRelation, ops: ArrayBuffer[NFPredicate]): Boolean = {
//    if (ops.isEmpty) true
//    else {
//      ops.map(isOkRel(_, rel.asInstanceOf[StoredRelationWithProperty])).reduce(_ && _)
//    }
//  }
//
//  def getRelsByFilter(ops: ArrayBuffer[NFPredicate], labels: Set[String], direction: Int): Iterator[Relationship[Id]] = {
//    val rels = {
//      if (ops.isEmpty) {
//        if (labels.isEmpty)
//          scan.allRelations()
//        else
//          scan.getRelationByType(labels.head)
//      }
//      else {
//        if (labels.isEmpty)
//          scan.allRelationsWithProperty
//        else
//          scan.getRelationByTypeWithProperty(labels.head)
//      }
//    }
//    if(ops.isEmpty) rels.map(scan.mapRelation)
//    else {
//      rels.map(scan.mapRelation).filter(filterRel(_, ops))
//    }
//  }
//  def getRelsByFilter(ops: ArrayBuffer[NFPredicate], labels: Set[String], direction: Int, isReturn: Boolean): Iterator[StoredRelation] = {
//    val rels = {
//      if (ops.isEmpty) {
//        if (labels.isEmpty)
//          scan.allRelations()
//        else
//          scan.getRelationByType(labels.head)
//      }
//      else {
//        if (labels.isEmpty)
//          scan.allRelationsWithProperty
//        else
//          scan.getRelationByTypeWithProperty(labels.head)
//      }
//    }
//    if(ops.isEmpty) rels
//    else {
//      rels.filter(filterRel(_, ops))
//
//    }
//  }
//
//
//
//  def getRelationById(nodeId: Long, direction: Int, labels: Set[String], ops: ArrayBuffer[NFPredicate]): Iterator[Relationship[Id]] ={
//
//    val rels = {
//      if (ops.isEmpty) {
//        if (labels.isEmpty)
//          scan.getRelationByNodeId(nodeId, direction)
//        else
//          scan.getRelationByNodeId(nodeId, direction, labels.head)
//      }
//      else {
//        if (labels.isEmpty)
//          scan.getRelationByNodeIdWithProperty(nodeId, direction)
//        else
//          scan.getRelationByNodeIdWithProperty(nodeId, direction, labels.head)
//      }
//    }
//    if(ops.isEmpty) rels.map(scan.mapRelation)
//    else {
//      rels.map(scan.mapRelation).filter(filterRel(_, ops))
//    }
//  }
//
//  def getRelationById2(nodeId: Long, direction: Int, labels: Set[String], ops: ArrayBuffer[NFPredicate], isReturn: Boolean): Iterator[StoredRelation] = {
//    if (isReturn) getRelationById2(nodeId, direction, labels, ops, isReturn).map(rel => scan.getRelationByIdWithProperty(rel.id))
//    else getRelationById2(nodeId, direction, labels, ops, isReturn).map(rel => StoredRelation(rel.id, rel.from, rel.to, rel.typeId))
//  }
//
//  def getRelationById(nodeId: Long, direction: Int, labels: Set[String], ops: ArrayBuffer[NFPredicate], isReturn: Boolean): Iterator[StoredRelation] ={
//
//    val rels = {
//      if (ops.isEmpty) {
//        if (labels.isEmpty)
//          scan.getRelationByNodeId(nodeId, direction)
//        else
//          scan.getRelationByNodeId(nodeId, direction, labels.head)
//      }
//      else {
//        if (labels.isEmpty)
//          scan.getRelationByNodeIdWithProperty(nodeId, direction)
//        else
//          scan.getRelationByNodeIdWithProperty(nodeId, direction, labels.head)
//      }
//    }
//    if(ops.isEmpty) rels
//    else {
//      rels.filter(filterRel(_, ops))
//
//    }
//  }
//
//
//
//  override def createElements(nodes: Array[IRNode], rels: Array[IRRelation[Id]]): Unit = writer.createElements(nodes, rels)
//  def mapNode(node: StoredNode): Node[Id] = scan.mapNode(node)
//
//  def mapRelation(relation: StoredRelation): Relationship[Id] = scan.mapRelation(relation)
//
//  def getNodeById(Id: Long): StoredNodeWithProperty = scan.getNodeById(Id)
//  def getRelById(Id: Long): StoredRelationWithProperty = scan.getRelationByIdWithProperty(Id)
//}
//
//trait HasStatistics{
//  // if unknown return -1
//
//  def getAllNodesCount(): Long = ???
//  def getNodesCountByLabel(label: String):Long = ???
//  def getNodesCountByLabelAndProperty(label: String, propertyName: String):Long = ???
//  def getNodesCountByLabelAndPropertys(label: String, propertyName: String*):Long = ???
//
//
//  def getAllRelsCount(): Long = ???
//  def getRelsCountByLabel(label: String):Long = ???
//  def getRelsCountByLabelAndProperty(label: String, propertyName: String):Long = ???
//  def getRelsCountByLabelAndPropertys(label: String, propertyName: String*):Long = ???
//}
//
//trait PandaPropertyGraphScan[Id] extends PropertyGraphScanner[Id] with HasStatistics{
//
//  /*
//  direction
//  0 -> Undirection
//  1 -> incoming
//  2 -> outgoing
//  */
//  val UNDIRECTED = 0
//  val IN = 1
//  val OUT = 2
//
//  def mapNode(node: StoredNode): Node[Id] = ???
//
//  def mapRelation(relation: StoredRelation): Relationship[Id] = ???
//
//  // relation
//
//  def getRelationByNodeId(nodeId: Long, direction: Int): Iterator[StoredRelation] = ???
//
//  def getRelationByNodeId(nodeId: Long, direction: Int, typeString: String): Iterator[StoredRelation] = ???
//
//  def getRelationByNodeIdWithProperty(nodeId: Long, direction: Int): Iterator[StoredRelationWithProperty] = ???
//
//  def getRelationByNodeIdWithProperty(nodeId: Long, direction: Int, typeString: String): Iterator[StoredRelationWithProperty] = ???
//
//  def getRelationByIdWithProperty(relId: Long): StoredRelationWithProperty = ???
//
//  def allRelations(): Iterator[StoredRelation] = ???
//
//  def allRelationsWithProperty: Iterator[StoredRelationWithProperty] = ???
//
//  def getRelationByType(typeString: String): Iterator[StoredRelation] = ???
//
//  def getRelationByTypeWithProperty(typeString: String): Iterator[StoredRelationWithProperty] = ???
//
//  //override
//
//  override def allNodes(): Iterable[Node[Id]] = getAllNodes().map(mapNode).toIterable
//
//  override def allRelationships(): Iterable[Relationship[Id]] = allRelations().map(mapRelation).toIterable
//
//  // node
//
//  def getNodeById(Id: Long): StoredNodeWithProperty = ???
//
//  def getNodesByLabel(labelString: String): Iterator[StoredNodeWithProperty] = ???
//
//  def getNodeIdsByLabel(labelString: String): Iterator[Id] = ???
//
//  def getAllNodes(): Iterator[StoredNodeWithProperty] = ???
//
//  def hasNodeLabels(Id: Long, labels: Set[String]): Boolean = labels.map(hasNodeLabel(Id, _)).reduce(_&&_)
//  def hasNodeLabel(Id: Long, label: String): Boolean = ???
//
//  // index
//  def isPropertyWithIndex(labels: Set[String], propertyName: String): (Int, String, Set[String], Long) = ???
//
//  def isPropertysWithIndex(labels: Set[String], propertyNames: Set[String]): (Int, String, Set[String], Long) = ???
//
//  def isPropertyWithIndex(label: String, propertyName: String): (Int, String, Set[String], Long) = ???
//
//  def isPropertysWithIndex(label: String, propertyName: Set[String]): (Int, String, Set[String], Long) = ???
//
//  def findNodeId(indexId: Int, value: Any): Iterator[Long] = ???
//
//  def findNode(indexId: Int, value: Any): Iterator[StoredNodeWithProperty] = ???
//
//  def findRangeNodeId(indexId: Int, from: Double, to: Double, fromClose:Boolean = false, toClose:Boolean = false): Iterator[Long] = ???
//
//  def findRangeNode(indexId: Int, from: Double, to: Double, fromClose:Boolean = false, toClose:Boolean = false): Iterator[StoredNodeWithProperty] = ???
//
//  def startWithNodeId(indexId: Int, start: String): Iterator[Long] = ???
//
//  def startWithNode(indexId: Int, start: String): Iterator[StoredNodeWithProperty] = ???
//
//  //Int <------>String
//
//  def getRelationPropertyNameById(keyId: Int): String = ???
//  def getRelationPropertyIdByName(name: String): Int = ???
//  def getRelationTypeNameById(keyId: Int): String = ???
//  def getRelationTypeIdByName(name: String): Int = ???
//
//
//  def getNodePropertyNameById(keyId: Int): String = ???
//  def getNodePropertyIdByName(name: String): Int = ???
//  def getNodeLabelNameById(keyId: Int): String = ???
//  def getNodeLabelIdByName(name: String): Int = ???
//  def getNodeLabelsNameByIds(keyId: Array[Int]): Array[String] = keyId.map(getNodeLabelNameById)
//
//
//}
