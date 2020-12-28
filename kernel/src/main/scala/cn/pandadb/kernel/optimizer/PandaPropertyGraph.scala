package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.kv.{NFBinaryPredicate, NFEquals, NFGreaterThan, NFGreaterThanOrEqual, NFLabels, NFLessThan, NFLessThanOrEqual, NFLimit, NFPredicate}
import cn.pandadb.kernel.optimizer.LynxType.{LynxNode, LynxRelationship}
import org.opencypher.lynx.{LynxPlannerContext, LynxRecords, LynxSession, LynxTable, PropertyGraphScanner, RecordHeader}
import org.opencypher.lynx.graph.{LynxPropertyGraph, ScanGraph}
import org.opencypher.lynx.plan.Filter
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}
import org.opencypher.okapi.ir.api.expr.{EndNode, NodeVar, RelationshipVar, StartNode}
import org.parboiled.scala.utils.Predicate

import scala.collection.mutable.ArrayBuffer



class PandaPropertyGraph[Id](scan: PandaPropertyGraphScan[Id])(implicit override val session: LynxSession) extends ScanGraph[Id](scan)(session) {
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
    if (predicate.nonEmpty) predicate.map(isOkNodes(_, node)).reduce(_&&_)
    else true
  }

  def getNodeCnt(predicate: Array[NFPredicate], labels: Set[String]): Long = {
    200
  }

  def getRelCnt(predicate: Array[NFPredicate], label: String, direction: Int): Long = {
    24
  }

  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String]): Iterable[Node[Id]] ={
    val node1 = LynxNode(1,Set("person"), "name" -> CypherValue("bob"), "age" -> CypherValue(40))
    val node2 = LynxNode(1,Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(20))
    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
    val nodes:Map[Long, LynxNode] = Map(1L->node1, 2L -> node2, 3L->node3)
    nodes.values.map(_.asInstanceOf[Node[Id]]).filter(filterByPredicates(_, predicate))
  }


  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String], sk: Int): Iterable[Node[Id]] = {


    //todo test
    val nodes = {
      if (labels.nonEmpty) {
        if (predicate.nonEmpty) {
          val (indexNfp, nfp) = findindexPredicate(predicate, labels)
          val tempnodes = {
            if (indexNfp.nonEmpty) indexNfp.map(scan.allNodes(_, labels).toSeq).reduce(_.intersect(_))
            else scan.allNodes(labels, false)
          }
          tempnodes.filter(filterByPredicates(_, nfp))
        }
        else scan.allNodes(labels, false)
      }
      else scan.allNodes()
    }
    nodes
  }

  def getNodesByFilter(predicate: Array[NFPredicate], labels: Set[String], nodeVar: NodeVar): LynxRecords = {

    new LynxRecords(
      RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name)),
      LynxTable(Seq(nodeVar.name -> CTNode), getNodesByFilter(predicate, labels).map(Seq(_)))
    )

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
        val (indexNfp, nfp) = findindexPredicate(predicateNew, labels.toSet)

        //getnodes by index
        val tempnodes = {
          if (indexNfp.nonEmpty) indexNfp.map(scan.allNodes(_, labels.distinct.toSet).toSeq).reduce(_.intersect(_))
          else scan.allNodes(labels.toSet, false)
        }
        //if (nfp.nonEmpty)
        //val tempnodes: Iterable[Node[Id]] = scan.allNodes(labels.toSet, false)

        //filternodes by noneindex
        if (size > 0 )
          tempnodes.filter(filterByPredicates(_, nfp)).take(size.toInt)
        else
          tempnodes.filter(filterByPredicates(_, nfp))
        //tempnodes
      }
      else scan.allNodes(labels.toSet, false)
    }
    new LynxRecords(
      RecordHeader(Map(NodeVar(name)(CTNode) -> name)),
      LynxTable(Seq(name -> CTNode), nodes.map(Seq(_)))
    )
  }


/*  def getRelByStartNodeId(sourceId: Long, direction:Int): Iterable[LynxRelationship] = {
    scan.getRelByStartNodeId(sourceId, direction)
  }*/

  def isOkRel(rel: LynxRelationship, Ops: ArrayBuffer[Filter]) = ???
  def getRelByStartNodeId(sourceId: Long, direction:Int, label: Set[String], Ops: ArrayBuffer[NFPredicate]): Iterable[Relationship[Id]] = {

    if (Ops.nonEmpty) {
      //todo
    }
    null
  }

  def getRelByStartNodeId(sourceId: Long, direction:Int, label: Set[String]): Iterable[Relationship[Id]] = {
    //todo test
    Array(LynxRelationship(1, 1, 2, "knows")).filter(_.startId ==sourceId).map(_.asInstanceOf[Relationship[Id]])
    //if (label.nonEmpty) scan.getRelByStartNodeId(sourceId, direction, label.head)
    //else scan.getRelByStartNodeId(sourceId, direction)
  }

  def getRelByEndNodeId(targetId: Long, direction:Int, label: Set[String]): Iterable[Relationship[Id]] = {
    if (label.nonEmpty) scan.getRelByEndNodeId(targetId, direction, label.head)
    else scan.getRelByEndNodeId(targetId, direction)
  }

  def getNodeById(Id: Long, labels: Set[String], Ops: ArrayBuffer[NFPredicate]): Option[Node[Id]] = {
    //todo filter nodes by label and Ops
    //Option[getNodeById(Id)]
    //Some(getNodeById(Id: Long))
    val node = getNodeById(Id)
    if (node.labels.equals(labels) && filterByPredicates(node, Ops.toArray)) Some(node)
    else None

  }
  //1 Map("name" -> "bob", "age" -> 40), "person"
  //2 (Map("name" -> "alex", "age" -> 20), "person")
  //3 Map("name" -> "simba", "age" -> 10), "worker")
  //("knows", 1L, 2L, Map())
  def getNodeById(Id: Long): Node[Id] = {
    //LynxNode(1,Set("bob"))
    val node1 = LynxNode(1,Set("person"), "name" -> CypherValue("bob"), "age" -> CypherValue(40))
    val node2 = LynxNode(1,Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(20))
    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
    val nodes:Map[Long, LynxNode] = Map(1L->node1, 2L -> node2, 3L->node3)

    nodes.get(Id).get.asInstanceOf[Node[Id]]

    //scan.getNodeById(Id)
    //todo getNodesById
  }

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
  def getRelsByFilter(labels: Set[String], direction: Int): Iterable[Relationship[Id]] = {
    if (labels.nonEmpty) scan.getRelsByFilter(labels.head, direction)
    else scan.getRelsByFilter(direction)
  }

  def getRelsByFilter(Ops: ArrayBuffer[NFPredicate], labels: Set[String], direction: Int): Iterable[Relationship[Id]] = {
    //todo filter rel
    //getRelsByFilter(labels, direction)
    Array(LynxRelationship(1, 1, 2, "knows").asInstanceOf[Relationship[Id]])
  }

  def getNodesByFilter(Ops: ArrayBuffer[NFPredicate], labels: Set[String]): Iterable[Node[Id]] = {
    val node3 = LynxNode(1,Set("worker"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
    //todo filter Nodes
    Array(node3).map(_.asInstanceOf[Node[Id]])
    //scan.allNodes()
  }

}



trait PandaPropertyGraphScan[Id] extends PropertyGraphScanner[Id] {
  //def isPropertyWithIndex(labels: Set[String], propertyName: String): Boolean = ???

  def isPropertyWithIndex(label: String, propertyName: String): Boolean = ???
  def isPropertysWithIndex(label: String, propertyName: String *): Boolean = ???

  def getAllNodesCount(): Long = ???
  def getNodesCountByLabel(label: String):Long = ???
  def getNodesCountByLabelAndProperty(label: String, propertyName: String):Long = ???
  def getNodesCountByLabelAndPropertys(label: String, propertyName: String*):Long = ???


  def getAllRelsCount(): Long = ???
  def getRelsCountByLabel(label: String):Long = ???
  def getRelsCountByLabelAndProperty(label: String, propertyName: String):Long = ???
  def getRelsCountByLabelAndPropertys(label: String, propertyName: String*):Long = ???


  // def isLabelWithIndex(label: String): Boolean = ???

 // def isPopertysWithIndex(propertyName1: String, propertyName2: String): Boolean = ???

 // def isLabelAndPropertyWithIndex(propertyName: String, label: String): Boolean = ???

  //def getRecorderNumbersFromProperty(labels: Set[String], propertyName: String): Int = ???

 // def getRecorderNumbersFromLabel(label: String): Int = ???
  /*
  direction
  0 -> Undirection
  1 -> incoming
  2 -> outgoing
  */

  def getRelByStartNodeId(sourceId: Long, direction:Int, label: String): Iterable[Relationship[Id]] = ???
  def getRelByStartNodeId(sourceId: Long, direction:Int): Iterable[Relationship[Id]] = ???
  def getRelByEndNodeId(targetId: Long, direction:Int, label: String): Iterable[Relationship[Id]] = ???
  def getRelByEndNodeId(targetId: Long, direction:Int): Iterable[Relationship[Id]] = ???

  def getRelsByFilter(labels: String, direction: Int): Iterable[Relationship[Id]] = ???

  def getRelsByFilter(direction: Int): Iterable[Relationship[Id]] = ???

  def getNodeById(Id: Long): Node[Id] = ???

  def allNodes(predicate: NFPredicate, labels: Set[String]): Iterable[Node[Id]] = ???


  def getRelByStartNodeIdWithProps(sourceId: Long, direction:Int, label: String): Iterable[Relationship[Id]] = ???
  def getRelByStartNodeIdWithProps(sourceId: Long, direction:Int): Iterable[Relationship[Id]] = ???
  def getRelByEndNodeIdWithProps(targetId: Long, direction:Int, label: String): Iterable[Relationship[Id]] = ???
  def getRelByEndNodeIdWithProps(targetId: Long, direction:Int): Iterable[Relationship[Id]] = ???

  def getRelsByFilterWithProps(labels: String, direction: Int): Iterable[Relationship[Id]] = ???

  def getRelsByFilterWithProps(direction: Int): Iterable[Relationship[Id]] = ???

  def getNodeByIdWithProps(Id: Long): Node[Id] = ???

  def allNodesWithProps(predicate: NFPredicate, labels: Set[String]): Iterable[Node[Id]] = ???

}
