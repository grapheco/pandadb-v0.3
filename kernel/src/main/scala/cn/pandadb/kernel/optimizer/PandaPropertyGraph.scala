package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.optimizer.NFEquals
import cn.pandadb.kernel.optimizer.LynxType.{LynxNode, LynxRelationship}
import org.opencypher.lynx.{LynxPlannerContext, LynxRecords, LynxSession, LynxTable, PropertyGraphScanner, RecordHeader}
import org.opencypher.lynx.graph.{LynxPropertyGraph, ScanGraph, WritableScanGraph}
import org.opencypher.lynx.ir.{IRNode, IRRelation, PropertyGraphWriter, WritablePropertyGraph}
import org.opencypher.lynx.plan.Filter
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}
import org.opencypher.okapi.ir.api.expr.{EndNode, NodeVar, RelationshipVar, StartNode}
import org.parboiled.scala.utils.Predicate

import scala.collection.mutable.ArrayBuffer



class PandaPropertyGraph[Id](scan: PandaPropertyGraphScan[Id], writer: PropertyGraphWriter[Id])(implicit override val session: LynxSession)
  //extends ScanGraph[Id](scan)(session)
  extends WritableScanGraph[Id](scan, writer)(session){
  //def getRecorderNumberFromPredicate(predicate: NFPredicate): Int = ???

  def isNFPredicateWithIndex(predicate: NFPredicate, labels: Set[String]): Boolean = {

  /*  predicate match{
      case x:NFEquals => scan.isPropertyWithIndex(labels, x.propName)
      case x:NFGreaterThan => scan.isPropertyWithIndex(labels, x.propName)
      case x:NFGreaterThanOrEqual => scan.isPropertyWithIndex(labels, x.propName)
      case x:NFLessThan => scan.isPropertyWithIndex(labels, x.propName)
      case x:NFLessThanOrEqual => scan.isPropertyWithIndex(labels, x.propName)
    }*/
    true

  }

  //def


  def isNFPredicatesWithIndex(predicate: Array[NFPredicate]): Boolean = {
    //predicate.map()
    val (predicateNew, labels) = findLabelPredicate(predicate)

    if (predicateNew.nonEmpty) predicateNew.map(isNFPredicateWithIndex(_, labels.distinct.toSet)).reduce(_|_)
    else true
  }

  def findLabelPredicate(predicate: Array[NFPredicate]): (Array[NFPredicate], Seq[String]) = {
    var nps: ArrayBuffer[NFPredicate] = ArrayBuffer[NFPredicate]()
    var labels: Seq[String] = Seq[String]()
    predicate.foreach(u => {
      u match {
        case x: NFLabels => {
          labels ++= x.labels
        }
        case x: NFPredicate => nps += x
      }
    })
    nps.toArray -> labels
  }

  def findLimitPredicate(predicate: Array[NFPredicate]):(ArrayBuffer[NFPredicate], Long) = {
    var nps: ArrayBuffer[NFPredicate] = ArrayBuffer[NFPredicate]()
    var limit: Long = -1
    predicate.foreach(u => {
      u match {
        case x: NFLimit => {
          limit = x.size
        }
        case x: NFPredicate => nps += x
      }
    })
    nps -> limit
  }
  def findindexPredicate(predicate: Array[NFPredicate], labels: Set[String]):(Array[NFPredicate], Array[NFPredicate]) = {
  /*  var npsWithIndex: ArrayBuffer[NFPredicate] = ArrayBuffer[NFPredicate]()
    var nps: ArrayBuffer[NFPredicate] = ArrayBuffer[NFPredicate]()
    predicate.foreach(u => {
      if
    })*/
    predicate.filter(isNFPredicateWithIndex(_, labels)) -> predicate.filter(!isNFPredicateWithIndex(_, labels))
  }




  /*  def findFirstPredicate(predicate: Array[NFPredicate]): ( Array[NFPredicate], NFPredicate) = {

    }*/




  def filterByPredicates(node: Node[Id], predicate: Array[NFPredicate]): Boolean = {
    if (predicate.nonEmpty) predicate.map(isOkNode(_, node)).reduce(_&&_)
    else true
  }

  def getNodeCnt(predicate: Array[NFPredicate], labels: Set[String]): Long = {
    if(labels.isEmpty) scan.getAllNodesCount()
    else{
      labels.map(scan.getNodesCountByLabel).min
    }
  }

  def getRelCnt(predicate: Array[NFPredicate], label: String, direction: Int): Long = {
    if (label ==null) scan.getAllRelsCount()
    else scan.getRelsCountByLabel(label)
  }

/*  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String]): Iterable[Node[Id]] ={
    val node1 = LynxNode(1,Set("person"), "name" -> CypherValue("bob"), "age" -> CypherValue(40))
    val node2 = LynxNode(1,Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(20))
    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
    val nodes:Map[Long, LynxNode] = Map(1L->node1, 2L -> node2, 3L->node3)
    nodes.values.map(_.asInstanceOf[Node[Id]]).filter(filterByPredicates(_, predicate))
  }

  def getNodesByFilter(prediates: Array[NFPredicate], name: String, nodeCypherType: CTNode): LynxRecords = {
    val node1 = LynxNode(1,Set("person"), "name" -> CypherValue("bob"), "age" -> CypherValue(40))
    val node2 = LynxNode(1,Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(20))
    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
    val nodes:Map[Long, LynxNode] = Map(1L->node1, 2L -> node2, 3L->node3)
    nodes.values.map(_.asInstanceOf[Node[Id]]).filter(filterByPredicates(_, prediates))
    new LynxRecords(null, null)
  }
  def getNodesByFilter(prediates: Array[NFPredicate],labels: Set[String], varNode: NodeVar): LynxRecords = {
    val node1 = LynxNode(1,Set("person"), "name" -> CypherValue("bob"), "age" -> CypherValue(40))
    val node2 = LynxNode(1,Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(20))
    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
    val nodes:Map[Long, LynxNode] = Map(1L->node1, 2L -> node2, 3L->node3)
    nodes.values.map(_.asInstanceOf[Node[Id]]).filter(filterByPredicates(_, prediates))
    new LynxRecords(null, null)
  }*/


//  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String], sk: Int): Iterable[Node[Id]] = {
//
//
//    //todo test
//    val nodes = {
//      if (labels.nonEmpty) {
//        if (predicate.nonEmpty) {
//          val (indexNfp, nfp) = findindexPredicate(predicate, labels)
//          val tempnodes = {
//            if (indexNfp.nonEmpty) indexNfp.map(scan.allNodes(_, labels).toSeq).reduce(_.intersect(_))
//            else scan.allNodes(labels, false)
//          }
//          tempnodes.filter(filterByPredicates(_, nfp))
//        }
//        else scan.allNodes(labels, false)
//      }
//      else scan.allNodes()
//    }
//    nodes
//  }
//
//  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String], nodeVar: NodeVar): LynxRecords = {
//
//    new LynxRecords(
//      RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name)),
//      LynxTable(Seq(nodeVar.name -> CTNode), getNodesByFilter(predicate, labels).map(Seq(_)))
//    )
//
//  }











  def isRangePredicate(p: NFPredicate): Boolean = {
    if (p.isInstanceOf[NFGreaterThan] || p.isInstanceOf[NFLessThan] || p.isInstanceOf[NFGreaterThanOrEqual] || p.isInstanceOf[NFLessThanOrEqual]) true
    else false
  }
  def UnionFilter(Ops: ArrayBuffer[NFPredicate]): ArrayBuffer[NFPredicate] = {
    var newOps: ArrayBuffer[NFPredicate] = new ArrayBuffer[NFPredicate]()
    if (Ops.isEmpty) new ArrayBuffer[NFPredicate]()
    else {
      Ops.filter(isRangePredicate(_)).groupBy(_.asInstanceOf[NFBinaryPredicate].getName())
    }
    null
  }


  //******************************************************************************************
  //********************************nodes*****************************************************
  //******************************************************************************************


  def isOkNode(p: NFPredicate, node: Node[Id]): Boolean = {
    p match {
      case x:NFGreaterThanOrEqual => if(node.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] >= x.value.asInstanceOf[Int]) true else false
      case x:NFLessThanOrEqual => if(node.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] <= x.value.asInstanceOf[Int]) true else false
      case x:NFEquals => {
        if(node.properties.get(x.propName).get.getValue.get.equals(x.value.anyValue) ) {
          true
        }
        else {
          false
        }
      }
      case x:NFLessThan => if(node.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] < x.value.asInstanceOf[Int]) true else false
      case x:NFGreaterThan => if(node.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] > x.value.asInstanceOf[Int]) true else false
    }
  }

  def getNodeById(nodeId: Long, labels: Set[String], filterOP: ArrayBuffer[NFPredicate]): Option[Node[Id]] ={
    val node = scan.getNodeById(nodeId)
    if(labels.isEmpty){
      if(filterOP.isEmpty) Some(node)
      else {
        if(filterOP.map(isOkNode(_, node)).reduce(_ && _)) Some(node)
        else None
      }
    }
    else{
      if (!labels.map(node.labels.contains(_)).reduce(_ && _)) None
      else{
        if(filterOP.isEmpty) Some(node)
        else {
          if(filterOP.map(isOkNode(_, node)).reduce(_ && _)) Some(node)
          else None
        }
      }
    }
  }

  def filterNode(node: Node[Id], ops: ArrayBuffer[NFPredicate]): Boolean = {
    if (ops.isEmpty) true
    else {
      ops.map(isOkNode(_, node)).reduce(_ && _)
    }
  }

  def filterNode(node: Node[Id], labels: Set[String]): Boolean = {
    if (labels.isEmpty) true
    else {
      labels.map(node.labels.contains(_)).reduce(_ && _)
    }
  }

  def getPropNames(ops: ArrayBuffer[NFPredicate]): Set[String] = {
    ops.map(_.asInstanceOf[NFBinaryPredicate].getPropName()).toSet
  }

  def getNodesByFilter(predicate: Array[NFPredicate], name: String, nodeCypherType: CTNode): LynxRecords = {


    val (predicateNew1, labels) = findLabelPredicate(predicate)
    new LynxRecords(RecordHeader(Map(NodeVar(name)(CTNode) -> name)),
      LynxTable(Seq(name -> CTNode), getNodesByFilter(predicateNew1, labels.toSet).map(Seq(_)))
    )
  }

  def findIndexIdAndValue(ops: ArrayBuffer[NFPredicate], labels: Set[String]): (Int, Any) = ???

  def getNodesByFilter(labels: Set[String]): Iterable[Node[Id]] = {
    if (labels.size ==1) scan.getNodesByLabel(labels.head)
    else {
      val label = labels.toArray.map(x => x -> scan.getNodesCountByLabel(x)).minBy(_._2)._1
      scan.getNodesByLabel(label).filter(filterNode(_, labels -- Set(label)))
    }
  }

  def getNodesByFilter(predicate: Array[NFPredicate],labels: Set[String], nodeVar: NodeVar): LynxRecords = {

    new LynxRecords(RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name)),
      LynxTable(Seq(nodeVar.name -> CTNode), getNodesByFilter(predicate, labels).map(Seq(_)))
        )
  }

  def getNodesByFilter(ops: Array[NFPredicate], labels: Set[String]): Iterable[Node[Id]] = {
    if(labels.isEmpty){
      if(ops.isEmpty)
        scan.allNodes()
      else{
        val (opsNew, size) = findLimitPredicate(ops.toArray)
        if (size > 0 )
          scan.allNodes().filter(filterNode(_, opsNew)).take(size.toInt)
        else
          scan.allNodes().filter(filterNode(_, opsNew))
      }
    }
    else {
      if (ops.isEmpty){
        getNodesByFilter(labels)
      }
      else{
        val (opsNew, size) = findLimitPredicate(ops.toArray)
        val eqlops = opsNew.filter(_.isInstanceOf[NFEquals]).map(_.asInstanceOf[NFEquals]).map(x => x.propName -> x.value.anyValue)
        val rangeops = opsNew.filter(!_.isInstanceOf[NFEquals]).map(_.asInstanceOf[NFBinaryPredicate]).map(x => {
          x.getPropName()->(x.getValue().anyValue,x.getType())
        })
        if(eqlops.isEmpty&&rangeops.isEmpty){
          if (size > 0) getNodesByFilter(labels).take(size.toInt)
          else getNodesByFilter(labels)
        }
        else {
          if (eqlops.isEmpty){
            val (indexId,label,props, cnt) = scan.isPropertysWithIndex(labels, rangeops.map(_._1).toSet)
            if (indexId > 0) {
              val (v,t) = props.toSeq.map(rangeops.toMap.get(_)).head.get
              val max = {
                if (v.isInstanceOf[Int]) Int.MaxValue
                else Float.MaxValue
              }

              val min = {
                if (v.isInstanceOf[Int]) Int.MinValue
                else Float.MinValue
              }

              val nodes = t match {
                case "<=" => scan.findRangeNode(indexId, min, v)
                case "<" => scan.findRangeNode(indexId, min, v)
                case ">=" => scan.findRangeNode(indexId, v, max)
                case ">" => scan.findRangeNode(indexId, v, max)
              }
              nodes.filter(filterNode(_, opsNew))
            }
            else {
              getNodesByFilter(labels).filter(filterNode(_, opsNew))
            }
          }
          else{
            val (indexId,label, props, cnt) = scan.isPropertysWithIndex(labels, eqlops.map(_._1).toSet)
            if (indexId > 0) {
              val v = props.toSeq.map(eqlops.toMap.get(_)).head
              scan.findNode(indexId, v.get).filter(filterNode(_, opsNew))
            }
            else
              getNodesByFilter(labels).filter(filterNode(_, opsNew))
          }
        }
      }
    }
  }









  //******************************************************************************************
  //********************************rels*****************************************************
  //******************************************************************************************

  def isOkRel(p: NFPredicate, rel: Relationship[Id]): Boolean = {
    p match {
      case x:NFGreaterThanOrEqual => if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] >= x.value.asInstanceOf[Int]) true else false
      case x:NFLessThanOrEqual => if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] <= x.value.asInstanceOf[Int]) true else false
      case x:NFEquals => {
        if(rel.properties.get(x.propName).get.getValue.get.equals(x.value.anyValue) ) {
          true
        }
        else {
          false
        }
      }
      case x:NFLessThan => if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] < x.value.asInstanceOf[Int]) true else false
      case x:NFGreaterThan => if(rel.properties.get(x.propName).get.getValue.get.asInstanceOf[Int] > x.value.asInstanceOf[Int]) true else false
    }
  }

  def filterRel(rel: Relationship[Id], ops: ArrayBuffer[NFPredicate]): Boolean = {
    if (ops.isEmpty) true
    else {
      ops.map(isOkRel(_, rel)).reduce(_ && _)
    }
  }

  def getRelsByFilter(ops: ArrayBuffer[NFPredicate], labels: Set[String], direction: Int): Iterable[Relationship[Id]] = {
    val rels = {
      if (ops.isEmpty) {
        if (labels.isEmpty)
          scan.allRelations()
        else
          scan.getRelationByType(labels.head)
      }
      else {
        if (labels.isEmpty)
          scan.allRelationsWithProperty
        else
          scan.getRelationByTypeWithProperty(labels.head)
      }
    }
    if(ops.isEmpty) rels
    else {
      rels.filter(filterRel(_, ops))
    }
  }

  def getRelationById(nodeId: Long, direction: Int, labels: Set[String], ops: ArrayBuffer[NFPredicate]): Iterable[Relationship[Id]] ={

    val rels = {
      if (ops.isEmpty) {
        if (labels.isEmpty)
          scan.getRelationByNodeId(nodeId, direction)
        else
          scan.getRelationByNodeId(nodeId, direction, labels.head)
      }
      else {
        if (labels.isEmpty)
          scan.getRelationByNodeIdWithProperty(nodeId, direction)
        else
          scan.getRelationByNodeIdWithProperty(nodeId, direction, labels.head)
      }
    }
    if(ops.isEmpty) rels
    else {
      rels.filter(filterRel(_, ops))
    }
  }

  override def createElements(nodes: Array[IRNode], rels: Array[IRRelation[Id]]): Unit = writer.createElements(nodes, rels)
}

trait HasStatistics{
  // if unknown return -1

  def getAllNodesCount(): Long = ???
  def getNodesCountByLabel(label: String):Long = ???
  def getNodesCountByLabelAndProperty(label: String, propertyName: String):Long = ???
  def getNodesCountByLabelAndPropertys(label: String, propertyName: String*):Long = ???


  def getAllRelsCount(): Long = ???
  def getRelsCountByLabel(label: String):Long = ???
  def getRelsCountByLabelAndProperty(label: String, propertyName: String):Long = ???
  def getRelsCountByLabelAndPropertys(label: String, propertyName: String*):Long = ???
}

trait PandaPropertyGraphScan[Id] extends PropertyGraphScanner[Id] with HasStatistics{

  /*
  direction
  0 -> Undirection
  1 -> incoming
  2 -> outgoing
  */
  val UNDIRECTION = 0
  val IN = 1
  val OUT = 2

  // relation

  def getRelationByNodeId(nodeId: Long, direction: Int): Iterable[Relationship[Id]] = ???

  def getRelationByNodeId(nodeId: Long, direction: Int, typeString: String): Iterable[Relationship[Id]] = ???

  def getRelationByNodeIdWithProperty(nodeId: Long, direction: Int): Iterable[Relationship[Id]] = ???

  def getRelationByNodeIdWithProperty(nodeId: Long, direction: Int, typeString: String): Iterable[Relationship[Id]] = ???

  def allRelations(): Iterable[Relationship[Id]] = ???

  def allRelationsWithProperty: Iterable[Relationship[Id]] = ???

  def getRelationByType(typeString: String): Iterable[Relationship[Id]] = ???

  def getRelationByTypeWithProperty(typeString: String): Iterable[Relationship[Id]] = ???

  // node

  def getNodeById(Id: Long): Node[Id] = ???

  def getNodesByLabel(labelString: String): Iterable[Node[Id]] = ???

  def allNodes(): Iterable[Node[Id]] = ???

  // index
  def isPropertyWithIndex(labels: Set[String], propertyName: String): (Int, String, Set[String], Long) = ???

  def isPropertysWithIndex(labels: Set[String], propertyNames: Set[String]): (Int, String, Set[String], Long) = ???

  def isPropertyWithIndex(label: String, propertyName: String): (Int, String, Set[String], Long) = ???

  def isPropertysWithIndex(label: String, propertyName: Set[String]): (Int, String, Set[String], Long) = ???

  def findNodeId(indexId: Int, value: Any): Iterable[Long] = ???

  def findNode(indexId: Int, value: Any): Iterable[Node[Id]] = ???

  def findRangeNodeId(indexId: Int, from: Any, to: Any): Iterable[Long] = ???

  def findRangeNode(indexId: Int, from: Any, to: Any): Iterable[Node[Id]] = ???

  def startWithNodeId(indexId: Int, start: String): Iterable[Long] = ???

  def startWithNode(indexId: Int, start: String): Iterable[Node[Id]] = ???

}
