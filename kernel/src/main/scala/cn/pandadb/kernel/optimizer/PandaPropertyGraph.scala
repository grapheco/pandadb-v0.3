package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.kv.{NFEquals, NFGreaterThan, NFGreaterThanOrEqual, NFLabels, NFLessThan, NFLessThanOrEqual, NFPredicate}
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

    predicateNew.map(isNFPredicateWithIndex(_, labels.distinct.toSet)).reduce(_|_)
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

  def getNodesByFilter(predicate: Array[NFPredicate], name: String, nodeCypherType: CTNode): LynxRecords = {


    val (predicateNew, labels) = findLabelPredicate(predicate)
    val nodes = {
      if(predicateNew.nonEmpty) predicateNew.map(scan.allNodes(_,  labels.distinct.toSet).toSeq).reduce(_.intersect(_))
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
