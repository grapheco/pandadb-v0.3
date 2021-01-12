//package org.opencypher.lynx.planning
//
//import cn.pandadb.kernel.optimizer.LynxType.LynxNode
//import cn.pandadb.kernel.optimizer.{NFLimit, NFPredicate, PandaPropertyGraph, Transformer}
//import cn.pandadb.kernel.store.{StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty, StoredValue, StoredValueNull}
//import org.opencypher.lynx.plan.PhysicalOperator
//import org.opencypher.lynx.{LynxRecords, LynxTable, RecordHeader}
//import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
//import org.opencypher.okapi.api.value.CypherValue
//import org.opencypher.okapi.api.value.CypherValue.{CypherNull, CypherValue, Node, Relationship}
//import org.opencypher.okapi.ir.api.expr.{Id, NodeVar, RelationshipVar, Var}
//import org.opencypher.okapi.logical.impl.{Directed, Direction, Incoming, LogicalOperator, Outgoing, SolvedQueryModel, Undirected}
//
//import scala.collection.Seq
//import scala.collection.mutable.ArrayBuffer
//
///*
//
//
//                                       :9H####@@@@@Xi
//                                      1@@@@@@@@@@@@@@8
//                                    ,8@@@@@@@@@B@@@@@@8
//                                   :B@@@@X3hi8Bs;B@@@@@Ah,
//              ,8i                  c@@@B:     1S ,M@@@@@@#8;
//             1AB35.i:               X@@8 .   SGhr ,A@@@@@@@@S
//             1@h31MX8                18Hhh3i .i3r ,A@@@@@@@@@5
//             ;@&i,58r5                 rGSS:     :B@@@@@@@@@@A
//              1#i  . 9i                 hX.  .: .5@@@@@@@@@@@1
//               sG1,  ,G53s.              9#Xi;hS5 3B@@@@@@@B1
//                .h8h.,A@@@MXSs,           #@H1:    3ssSSX@1
//                s ,@@@@@@@@@@@@Xhi,       r#@@X1s9M8    .GA981
//                ,. rS8H#@@@@@@@@@@#HG51;.  .h31i;9@r    .8@@@@BS;i;
//                 .19AXXXAB@@@@@@@@@@@@@@#MHXG893hrX#XGGXM@@@@@@@@@@MS
//                 s@@MM@@@hsX#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@&,
//               :GB@#3G@@Brs ,1GM@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@B,
//             .hM@@@#@@#MX 51  r;iSGAM@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@8
//           :3B@@@@@@@@@@@&9@h :Gs   .;sSXH@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@:
//       s&HA#@@@@@@@@@@@@@@M89A;.8S.       ,r3@@@@@@@@@@@@@@@@@@@@@@@@@@@r
//    ,13B@@@@@@@@@@@@@@@@@@@5 5B3 ;.         ;@@@@@@@@@@@@@@@@@@@@@@@@@@@i
//   5#@@#&@@@@@@@@@@@@@@@@@@9  .39:          ;@@@@@@@@@@@@@@@@@@@@@@@@@@@;
//   9@@@X:MM@@@@@@@@@@@@@@@#;    ;31.         H@@@@@@@@@@@@@@@@@@@@@@@@@@:
//    SH#@B9.rM@@@@@@@@@@@@@B       :.         3@@@@@@@@@@@@@@@@@@@@@@@@@@5
//      ,:.   9@@@@@@@@@@@#HB5                 .M@@@@@@@@@@@@@@@@@@@@@@@@@B
//            ,ssirhSM@&1;i19911i,.             s@@@@@@@@@@@@@@@@@@@@@@@@@@S
//               ,,,rHAri1h1rh&@#353Sh:          8@@@@@@@@@@@@@@@@@@@@@@@@@#:
//             .A3hH@#5S553&@@#h   i:i9S          #@@@@@@@@@@@@@@@@@@@@@@@@@A.
//
//     what? check my code?  That's impossible!!!
//
// */
//
//class PandaPhysicalOps {
//
//}
//
//trait TNode {
//
//}
//
//case class SourceNode() extends TNode{
//
//}
//
//case class TargetNode() extends TNode{
//
//}
//
//final case class  ScanNodes(isEnd: Boolean, nodeVar: Var, varMap: Map[Var, TNode], in: PhysicalOperator, next: PhysicalOperator, labels: Set[String], filterOP: ArrayBuffer[NFPredicate], isReturn: Boolean = true, isTable: Boolean = true) extends PhysicalOperator{
//
//  override lazy val recordHeader: RecordHeader = {
//    if (isEnd) RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name))
//    else next.recordHeader ++ RecordHeader(Map(NodeVar(nodeVar.name)(CTNode) -> nodeVar.name))
//  }
//  val recordHeaderMe: RecordHeader = RecordHeader.from(getNodeVar)
//
//  def getNodeVar(): NodeVar = {
//    NodeVar(nodeVar.name)(CTNode)
//  }
//
//
//  lazy val linTable: LinTable = {
//
//    if (isEnd){
//      val (opsNew, limit) = findLimitPredicate(filterOP.toArray)
//      if(limit>0) new LinTable(Seq(nodeVar.name), graph.asInstanceOf[PandaPropertyGraph[Id]].getNodesByFilter(opsNew.toArray, labels, isReturn).map(Seq(_)).take(limit.toInt), Seq(nodeVar.name -> CTNode), Seq(nodeVar.name -> isReturn))
//      else new LinTable(Seq(nodeVar.name), graph.asInstanceOf[PandaPropertyGraph[Id]].getNodesByFilter(opsNew.toArray, labels, isReturn).map(Seq(_)), Seq(nodeVar.name -> CTNode), Seq(nodeVar.name -> isReturn))
//    }
//    else {
//      next match {
//        case x:ScanNodes =>getNodesByTable(x.linTable)
//        case x:ScanRels =>getNodesByTable(x.linTable)
//      }
//    }
//  }
//
//
//  def getNodesByTable(t:LinTable):LinTable = {
//    val (opsNew, limit) = findLimitPredicate(filterOP.toArray)
//    val (rel, tNode) = varMap.filter(x => t.schema.contains(x._1.name)).head
//    val isSrc:Boolean = tNode match {
//      case SourceNode() => true
//      case TargetNode() => false
//    }
//
//    def isOkValue(row: Seq[_ <: StoredValue]): Boolean = {
//      val id = {
//        if (isSrc) t.cell(row, rel.name).asInstanceOf[StoredRelation].from
//        else t.cell(row, rel.name).asInstanceOf[StoredRelation].to
//      }
//      in.graph.asInstanceOf[PandaPropertyGraph[Id]].isOkNodesId(id, labels, opsNew)
//    }
//
//    val tempRecords1 = t.recordes.filter(isOkValue)
//
//
//    val records  = tempRecords1.map(row => {
//
//      val id = {
//        if (isSrc) t.cell(row, rel.name).asInstanceOf[StoredRelation].from
//        else t.cell(row, rel.name).asInstanceOf[StoredRelation].to
//      }
//      val node = in.graph.asInstanceOf[PandaPropertyGraph[Id]].getNodeById(id, isReturn)
//      row ++ Seq(node)
//    })
//
//
//    val newRecords = {
//      if (limit>0) records.take(limit.toInt)
//      else records
//    }
//
//    new LinTable(t.schema ++ Seq(nodeVar.name), newRecords, t.schemas ++ Seq(nodeVar.name -> CTNode), t.reSchemas ++Seq(nodeVar.name -> isReturn))
//
//  }
//
//  def mapStoredValue(v: StoredValue): CypherValue = {
//    v match{
//      case x:StoredNode =>
//      val y = in.graph.asInstanceOf[PandaPropertyGraph[Id]].getNodeById(x.id)
//        in.graph.asInstanceOf[PandaPropertyGraph[Id]].mapNode(y)
//      case x:StoredRelation =>
//        val y = in.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelById(x.id)
//        in.graph.asInstanceOf[PandaPropertyGraph[Id]].mapRelation(y)
//      case x:StoredNodeWithProperty =>in.graph.asInstanceOf[PandaPropertyGraph[Id]].mapNode(x)
//      case x:StoredRelationWithProperty =>in.graph.asInstanceOf[PandaPropertyGraph[Id]].mapRelation(x)
//    }
//  }
//  override lazy val table: LynxTable = {
//    //LynxTable(this.linTable.schemas, this.linTable.recordes.map(_.map(mapStoredValue)).toIterable)
//    LynxTable(this.linTable.selectSchema(), this.linTable.selectRecordes().map(_.map(mapStoredValue)).toIterable)
//  }
//  /*override lazy val table: LynxTable = {
//    if (isEnd) {
//      val records = getNodes()
//      LynxTable(Seq(nodeVar.name -> CTNode), records.toIterator.map(Seq(_)).toIterable)
//    }
//    else {
//      val (rel, tNode) = varMap.filter(x => next.table.physicalColumns.contains(x._1.name)).head
//      val isSrc:Boolean = tNode match {
//        case SourceNode() => true
//        case TargetNode() => false
//      }
//      val (opsNew, limit) = findLimitPredicate(filterOP.toArray)
//      val records:Iterable[Seq[_ <: CypherValue]]  = next.table.records.toIterator.map(row => {
//
//     /*   val id = tNode match {
//          case SourceNode() => next.table.cell(row, rel.name).asInstanceOf[Relationship[Long]].startId
//          case TargetNode() => next.table.cell(row, rel.name).asInstanceOf[Relationship[Long]].endId
//        }*/
//       val id = {
//         if (isSrc) next.table.cell(row, rel.name).asInstanceOf[Relationship[Long]].startId
//         else next.table.cell(row, rel.name).asInstanceOf[Relationship[Long]].endId
//       }
//
//
//        val node = in.graph.asInstanceOf[PandaPropertyGraph[Id]].getNodeById(id, labels, opsNew)
//        node match {
//            //rels.map(row ++ Seq(_))
//          case Some(value) => row ++ Seq(value)
//          case None => Seq(CypherNull)
//        }
//      }).toIterable
//
//      records.toIterator.filter(!_.equals(Seq(CypherNull))).toIterable
//
//      val newRecords = {
//        if (limit>0) records.toIterator.filter(!_.equals(Seq(CypherNull))).toIterable.take(limit.toInt)
//        else records.toIterator.filter(!_.equals(Seq(CypherNull))).toIterable
//      }
//
//
//      LynxTable(next.table.schema ++ Seq(nodeVar.name -> CTNode), newRecords)
//    }
//  }*/
//  def getRecords: LynxRecords = {
//    if (graph.isInstanceOf[PandaPropertyGraph[Id]]) {
//      graph.asInstanceOf[PandaPropertyGraph[Id]].getNodesByFilter(filterOP.toArray, labels, nodeVar.asInstanceOf[NodeVar])
//
//    }
//    else {
//      throw new Exception("graph is not an instance of PandaScanGraph")
//    }
//  }
//
//  def getNodes(): Iterable[Node[Id]] = {
//    graph.asInstanceOf[PandaPropertyGraph[Id]].getNodesByFilter(filterOP.toArray, labels).toIterable
//
//  }
//
//  def getRecordsNumbers: Long = {
//    graph.asInstanceOf[PandaPropertyGraph[Id]].getNodeCnt(filterOP.toArray, labels)
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
//
//}
//
//final case class  ScanRels(isEnd: Boolean,
//                           sVar: Var,
//                           rel: Var,
//                           tVar: Var,
//                           //scanType: ScanType,
//                           next: PhysicalOperator,
//                           direction: Direction, labels: Set[String],
//                           filterOP: ArrayBuffer[NFPredicate], isReturn: Boolean = true, isTable: Boolean = true) extends PhysicalOperator{
//
//  override val graph = next.graph
//
//  override lazy val recordHeader: RecordHeader = {
//    if (isEnd) RecordHeader(Map(RelationshipVar(rel.name)(CTRelationship) -> rel.name))
//    else next.recordHeader ++ RecordHeader(Map(RelationshipVar(rel.name)(CTRelationship) -> rel.name))
//  }
//
//  var dir: Int = direction match {
//    case Undirected => 0
//    case Incoming => 1
//    case Outgoing => 2
//  }
//
//  lazy val linTable: LinTable = {
//    if (isEnd){
//      val (opsNew, limit) = findLimitPredicate(filterOP.toArray)
//      if (limit>0)
//        new LinTable(Seq(rel.name), next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelsByFilter(opsNew, labels, dir, isReturn).map(Seq(_)).take(limit.toInt), Seq(rel.name -> CTRelationship), Seq(rel.name -> isReturn))
//      else
//        new LinTable(Seq(rel.name), next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelsByFilter(opsNew, labels, dir, isReturn).map(Seq(_)), Seq(rel.name -> CTRelationship), Seq(rel.name -> isReturn))
//    }
//    else {
//      next match {
//        case x:ScanNodes =>getRelsByTable(x.linTable)
//        case x:ScanRels =>getRelsByTable(x.linTable)
//      }
//    }
//  }
//
//  def getRelsByTable(t:LinTable): LinTable = {
//    val isFirst = t.schema.contains(sVar.name)
//    val (opsNew, limit) = findLimitPredicate(filterOP.toArray)
//    val tdir = dir match {
//      case 0 => 0
//      case 1 => 2
//      case 2 => 1
//    }
//    val records = t.recordes.flatMap(row => {
//      if (isFirst) {
//        val id = t.cell(row, sVar.name).asInstanceOf[StoredNode].id
//        val rels = next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelationById(id, dir, labels, opsNew, isReturn)
//        rels.map(row ++ Seq(_))
//      }
//      else {
//
//        val id = t.cell(row, tVar.name).asInstanceOf[StoredNode].id
//        val rels = next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelationById(id, tdir, labels, opsNew, isReturn)
//        rels.map(row ++ Seq(_))
//      }
//
//    })
//    if(limit > 0)
//      new LinTable(t.schema ++ Seq(rel.name), records.take(limit.toInt), t.schemas ++ Seq(rel.name -> CTRelationship), t.reSchemas ++ Seq(rel.name -> isReturn))
//    else
//      new LinTable(t.schema ++ Seq(rel.name), records, t.schemas ++ Seq(rel.name -> CTRelationship), t.reSchemas ++ Seq(rel.name -> isReturn))
//  }
//
//  def mapStoredValue(v: StoredValue): CypherValue = {
//    v match{
//      case x:StoredNode =>
//        val y = graph.asInstanceOf[PandaPropertyGraph[Id]].getNodeById(x.id)
//        graph.asInstanceOf[PandaPropertyGraph[Id]].mapNode(y)
//      case x:StoredRelation =>
//        val y = graph.asInstanceOf[PandaPropertyGraph[Id]].getRelById(x.id)
//        graph.asInstanceOf[PandaPropertyGraph[Id]].mapRelation(y)
//      case x:StoredNodeWithProperty =>graph.asInstanceOf[PandaPropertyGraph[Id]].mapNode(x)
//      case x:StoredRelationWithProperty =>graph.asInstanceOf[PandaPropertyGraph[Id]].mapRelation(x)
//    }
//  }
//
//  override lazy val table: LynxTable = {
//    //LynxTable(this.linTable.schemas, this.linTable.recordes.map(_.map(mapStoredValue)).toIterable)
//    LynxTable(this.linTable.selectSchema(), this.linTable.selectRecordes().map(_.map(mapStoredValue)).toIterable)
//  }
//
///*  override lazy val table: LynxTable = {
//    if (isEnd) {
//      val records = next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelsByFilter(filterOP, labels, dir)
//      LynxTable(Seq(rel.name -> CTRelationship), records.map(Seq(_)).toIterable)
//    }
//    else {
//      val isFirst = next.table.physicalColumns.contains(sVar.name)
//      val records = next.table.records.toIterator.flatMap(row => {
//        if (isFirst) {
//          val id = next.table.cell(row, sVar.name).asInstanceOf[Node[Long]].id
//          val rels = next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelationById(id, dir, labels, filterOP)
//          rels.map(row ++ Seq(_))
//        }
//        else {
//          dir = dir match {
//            case 0 => 0
//            case 1 => 2
//            case 2 => 1
//          }
//          val id = next.table.cell(row, tVar.name).asInstanceOf[Node[Long]].id
//          val rels = next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelationById(id, dir, labels, filterOP)
//          rels.map(row ++ Seq(_))
//        }
//
//      }).toIterable
//
//      LynxTable(next.table.schema ++ Seq(rel.name -> CTRelationship), records)
//    }
//  }*/
//
//  def getRecordsNumbers: Long = {
//    next.graph.asInstanceOf[PandaPropertyGraph[Id]].getRelCnt(filterOP.toArray, labels.headOption.orNull, dir)
//
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
//
//}
//
//
//
