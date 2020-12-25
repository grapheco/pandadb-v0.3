package org.opencypher.lynx.planning

import cn.pandadb.kernel.kv.NFPredicate
import cn.pandadb.kernel.optimizer.LynxType.LynxNode
import cn.pandadb.kernel.optimizer.{PandaPropertyGraph, Transformer}
import org.opencypher.lynx.{LynxRecords, LynxTable, RecordHeader}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherNull, CypherValue, Node, Relationship}
import org.opencypher.okapi.ir.api.expr.{Id, NodeVar, RelationshipVar, Var}
import org.opencypher.okapi.logical.impl.{Directed, Direction, Incoming, LogicalOperator, Outgoing, SolvedQueryModel, Undirected}

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

/*


                                       :9H####@@@@@Xi
                                      1@@@@@@@@@@@@@@8
                                    ,8@@@@@@@@@B@@@@@@8
                                   :B@@@@X3hi8Bs;B@@@@@Ah,
              ,8i                  c@@@B:     1S ,M@@@@@@#8;
             1AB35.i:               X@@8 .   SGhr ,A@@@@@@@@S
             1@h31MX8                18Hhh3i .i3r ,A@@@@@@@@@5
             ;@&i,58r5                 rGSS:     :B@@@@@@@@@@A
              1#i  . 9i                 hX.  .: .5@@@@@@@@@@@1
               sG1,  ,G53s.              9#Xi;hS5 3B@@@@@@@B1
                .h8h.,A@@@MXSs,           #@H1:    3ssSSX@1
                s ,@@@@@@@@@@@@Xhi,       r#@@X1s9M8    .GA981
                ,. rS8H#@@@@@@@@@@#HG51;.  .h31i;9@r    .8@@@@BS;i;
                 .19AXXXAB@@@@@@@@@@@@@@#MHXG893hrX#XGGXM@@@@@@@@@@MS
                 s@@MM@@@hsX#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@&,
               :GB@#3G@@Brs ,1GM@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@B,
             .hM@@@#@@#MX 51  r;iSGAM@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@8
           :3B@@@@@@@@@@@&9@h :Gs   .;sSXH@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@:
       s&HA#@@@@@@@@@@@@@@M89A;.8S.       ,r3@@@@@@@@@@@@@@@@@@@@@@@@@@@r
    ,13B@@@@@@@@@@@@@@@@@@@5 5B3 ;.         ;@@@@@@@@@@@@@@@@@@@@@@@@@@@i
   5#@@#&@@@@@@@@@@@@@@@@@@9  .39:          ;@@@@@@@@@@@@@@@@@@@@@@@@@@@;
   9@@@X:MM@@@@@@@@@@@@@@@#;    ;31.         H@@@@@@@@@@@@@@@@@@@@@@@@@@:
    SH#@B9.rM@@@@@@@@@@@@@B       :.         3@@@@@@@@@@@@@@@@@@@@@@@@@@5
      ,:.   9@@@@@@@@@@@#HB5                 .M@@@@@@@@@@@@@@@@@@@@@@@@@B
            ,ssirhSM@&1;i19911i,.             s@@@@@@@@@@@@@@@@@@@@@@@@@@S
               ,,,rHAri1h1rh&@#353Sh:          8@@@@@@@@@@@@@@@@@@@@@@@@@#:
             .A3hH@#5S553&@@#h   i:i9S          #@@@@@@@@@@@@@@@@@@@@@@@@@A.

     what? check my code?  That's impossible!!!

 */

class PandaPhysicalOps {

}

trait TNode {

}

case class SourceNode() extends TNode{

}

case class TargetNode() extends TNode{

}

final case class  ScanNodes(isEnd: Boolean, nodeVar: Var, varMap: Map[Var, TNode], in: PhysicalOperator, next: PhysicalOperator, labels: Set[String], filterOP: ArrayBuffer[NFPredicate]) extends PhysicalOperator{

  override lazy val recordHeader: RecordHeader = {
    if (isEnd) RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name))
    else next.recordHeader ++ RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name))
  }
  val recordHeaderMe: RecordHeader = RecordHeader.from(getNodeVar)

  def getNodeVar(): NodeVar = {
    NodeVar(nodeVar.name)(CTNode)
  }


  override lazy val table: LynxTable = {
    if (isEnd) {
      val records = getNodes()
      LynxTable(Seq(nodeVar.name -> CTNode), records.map(Seq(_)))
    }
    else {
      val (rel, tNode) = varMap.filter(x => next.table.physicalColumns.contains(x._1.name)).head
      val records:Iterable[Seq[_ <: CypherValue]]  = next.table.records.map(row => {

        val id = tNode match {
          case SourceNode() => next.table.cell(row, rel.name).asInstanceOf[Relationship[Long]].startId
          case TargetNode() => next.table.cell(row, rel.name).asInstanceOf[Relationship[Long]].endId
        }


        val node = in.graph.asInstanceOf[PandaPropertyGraph[Id]].getNodeById(id, labels, filterOP)
        node match {
            //rels.map(row ++ Seq(_))
          case Some(value) => row ++ Seq(value)
          case None => Seq(CypherNull)
        }
      })


      LynxTable(next.table.schema ++ Seq(nodeVar.name -> CTNode), records.filter(!_.equals(Seq(CypherNull))))
    }
  }
  def getRecords: LynxRecords = {
    if (graph.isInstanceOf[PandaPropertyGraph[Id]]) {
      graph.asInstanceOf[PandaPropertyGraph[Id]].getNodesByFilter(filterOP.toArray, labels, nodeVar.asInstanceOf[NodeVar])

    }
    else {
      throw new Exception("graph is not an instance of PandaScanGraph")
    }
  }

  def getNodes(): Iterable[Node[Id]] = {
    graph.asInstanceOf[PandaPropertyGraph[Id]].getNodesByFilter(filterOP.toArray, labels)

  }

  def getRecordsNumbers: Long = {
    graph.asInstanceOf[PandaPropertyGraph[Id]].getNodeCnt(filterOP.toArray, labels)
  }

}

final case class  ScanRels(isEnd: Boolean,
                           sVar: Var,
                           rel: Var,
                           tVar: Var,
                           //scanType: ScanType,
                           next: PhysicalOperator,
                           direction: Direction, labels: Set[String],
                           filterOP: ArrayBuffer[NFPredicate]) extends PhysicalOperator{

  override val graph = next.graph

  override lazy val recordHeader: RecordHeader = {
    if (isEnd) RecordHeader(Map(NodeVar(rel.name)(CTRelationship) -> rel.name))
    else next.recordHeader ++ RecordHeader(Map(NodeVar(rel.name)(CTRelationship) -> rel.name))
  }

  val dir: Int = direction match {
    case Undirected => 0
    case Incoming => 1
    case Outgoing => 2
  }

  override lazy val table: LynxTable = {
    if (isEnd) {
      val records = next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelsByFilter(filterOP, labels, dir)
      LynxTable(Seq(rel.name -> CTRelationship), records.map(Seq(_)))
    }
    else {
      val isFirst = next.table.physicalColumns.contains(sVar.name)
      val records = next.table.records.flatMap(row => {
        if (isFirst) {
          val id = next.table.cell(row, sVar.name).asInstanceOf[Node[Long]].id
          val rels = next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelByStartNodeId(id, dir, labels)
          rels.map(row ++ Seq(_))
        }
        else {
          val id = next.table.cell(row, tVar.name).asInstanceOf[Node[Long]].id
          val rels = next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelByEndNodeId(id, dir, labels)
          rels.map(row ++ Seq(_))
        }

      })

      LynxTable(next.table.schema ++ Seq(rel.name -> CTRelationship), records)
    }
  }

  def getRecordsNumbers: Long = {
    next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelCnt(filterOP.toArray, labels.headOption.orNull, dir)

  }

}



