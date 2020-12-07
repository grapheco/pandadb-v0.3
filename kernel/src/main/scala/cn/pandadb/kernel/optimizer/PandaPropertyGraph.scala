package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.kv.{NFLabels, NFPredicate}
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



class PandaPropertyGraph[Id](scan: PandaPropertyGraphScan[Id])(implicit override val session: LynxSession) extends ScanGraph[Id](scan)(session) {
  def getRecorderNumberFromPredicate(predicate: NFPredicate): Int = ???

  def isNFPredicateWithIndex(predicate: NFPredicate): Boolean = {
    //predicate match {
      //case x:NFLabels => x.labels.map(scan.isLabelWithIndex(_)).reduce(_|_)
    //}
  }


  def isNFPredicatesWithIndex(predicate: Array[NFPredicate]): Boolean = {
    //predicate.map()
  }

  def getNodesByFilter(predicate: Array[NFPredicate], name: String, nodeCypherType: CTNode): LynxRecords = {
/*    var nodes:Seq[Node[Id]] = Seq[Node[Id]]()
    var isFirst: Boolean = true
    predicate.foreach(u =>{
      if(!isFirst){
        nodes = nodes.intersect(scan.allNodes(u).toSeq)
      }
      else {
        nodes = scan.allNodes(u).toSeq
        isFirst = false
      }
    })*/

   // val nodes = predicate.map(scan.allNodes(_).toSeq).reduce(_.intersect(_))
   // new LynxRecords(
   //   RecordHeader(Map(NodeVar(name)(CTNode) -> name)),
   //   LynxTable(Seq(name -> CTNode), nodes.map(Seq(_)))
   // )
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
