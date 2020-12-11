package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.kv.{NFEquals, NFGreaterThan, NFGreaterThanOrEqual, NFLabels, NFLessThan, NFLessThanOrEqual, NFLimit, NFPredicate}
import cn.pandadb.kernel.optimizer.costore.LynxNode
import org.opencypher.lynx.{LynxPlannerContext, LynxRecords, LynxSession, LynxTable, PropertyGraphScan, RecordHeader}
import org.opencypher.lynx.graph.{LynxPropertyGraph, ScanGraph}
import org.opencypher.lynx.planning.{PhysicalOperator, Start, TabularUnionAll}
import org.opencypher.okapi.api.graph.{Pattern, PatternElement, SourceEndNodeKey, SourceStartNodeKey}
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.{Node, Relationship}
import org.opencypher.okapi.ir.api.expr.{EndNode, NodeVar, RelationshipVar, StartNode}
import org.parboiled.scala.utils.Predicate

import scala.collection.mutable.ArrayBuffer



class PandaPropertyGraph[Id](scan: PandaPropertyGraphScan[Id])(implicit override val session: LynxSession) extends ScanGraph[Id](scan)(session) {
  //def getRecorderNumberFromPredicate(predicate: NFPredicate): Int = ???

  def isNFPredicateWithIndex(predicate: NFPredicate, labels: Set[String]): Boolean = {

    predicate match{
      case x:NFEquals => scan.isPropertyWithIndex(labels, x.propName)
      case x:NFGreaterThan => scan.isPropertyWithIndex(labels, x.propName)
      case x:NFGreaterThanOrEqual => scan.isPropertyWithIndex(labels, x.propName)
      case x:NFLessThan => scan.isPropertyWithIndex(labels, x.propName)
      case x:NFLessThanOrEqual => scan.isPropertyWithIndex(labels, x.propName)
    }

  }


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

  def findLimitPredicate(predicate: Array[NFPredicate]):(Array[NFPredicate], Long) = {
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
    nps.toArray -> limit
  }



/*  def findFirstPredicate(predicate: Array[NFPredicate]): ( Array[NFPredicate], NFPredicate) = {

  }*/

  def isOkNodes(p: NFPredicate, node: Node[Id]): Boolean = {
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

  def filterByPredicates(node: Node[Id], predicate: Array[NFPredicate]): Boolean = {
    predicate.map(isOkNodes(_, node)).reduce(_&&_)
  }

  def getNodesByFilter(predicate: Array[NFPredicate], name: String, nodeCypherType: CTNode): LynxRecords = {


    val (predicateNew1, labels) = findLabelPredicate(predicate)
    val (predicateNew, size) = findLimitPredicate(predicateNew1)
/*    val nodes = {
      if(predicateNew.nonEmpty) predicateNew.map(scan.allNodes(_,  labels.distinct.toSet).toSeq).reduce(_.intersect(_))
      else scan.allNodes(labels.toSet, false)
    }*/

    //val (predicateNew2, firstpredicate) =


    val nodes = {
      if (predicateNew.nonEmpty) {
        val tempnodes: Iterable[Node[Id]] = scan.allNodes(labels.toSet, false)
        if (size > 0 )
          tempnodes.filter(filterByPredicates(_, predicateNew)).take(size.toInt)
        else
          tempnodes.filter(filterByPredicates(_, predicateNew))
        //tempnodes
      }
      else scan.allNodes(labels.toSet, false)
    }
    new LynxRecords(
      RecordHeader(Map(NodeVar(name)(CTNode) -> name)),
      LynxTable(Seq(name -> CTNode), nodes.map(Seq(_)))
    )
  }
}



trait PandaPropertyGraphScan[Id] extends PropertyGraphScan[Id] {
  def isPropertyWithIndex(labels: Set[String], propertyName: String): Boolean = ???

  // def isLabelWithIndex(label: String): Boolean = ???

 // def isPopertysWithIndex(propertyName1: String, propertyName2: String): Boolean = ???

 // def isLabelAndPropertyWithIndex(propertyName: String, label: String): Boolean = ???

  //def getRecorderNumbersFromProperty(labels: Set[String], propertyName: String): Int = ???

 // def getRecorderNumbersFromLabel(label: String): Int = ???

  def allNodes(predicate: NFPredicate, labels: Set[String]): Iterable[Node[Id]] = ???

}
