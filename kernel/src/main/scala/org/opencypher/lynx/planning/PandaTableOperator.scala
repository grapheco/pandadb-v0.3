//package org.opencypher.lynx.planning
//
//import org.opencypher.lynx.LynxTable
//import org.opencypher.lynx.plan.{InnerJoin, JoinType, SimpleTableOperator}
//import org.opencypher.okapi.api.value.CypherValue.CypherValue
//
//import scala.collection.Seq
//
///*
//      (_\     /_)
//       ))   ((
//     .-"""""""-.
// /^\/  _.   _.  \/^\
// \(   /__\ /__\   )/
//  \,  \o_/_\o_/  ,/
//    \    (_)    /
//     `-.'==='.-'
//      __) - (__
//     /  `~~~`  \
//    /  /     \  \
//    \ :       ; /
//     \|==(*)==|/
//      :       :
//       \  |  /
//     ___)=|=(___
//    {____/ \____}
//   Boom Boom Boom !!!
//*/
//
//
//
///*
//   table a:
//   ╔════════╗
//   ║  node  ║
//   ╠════════╣
//   ║   1    ║
//   ║   2    ║
//   ╚════════╝
//   table b:
//   ╔═════╤════════╤════════╗
//   ║ rel │ source │ target ║
//   ╠═════╪════════╪════════╣
//   ║  1  │    1   │   2    ║
//   ║  2  │    2   │   3    ║
//   ╚═════╧════════╧════════╝
//   joinCols: a.node=b.source
//   expected:
//   ╔══════╤═════╤════════╤════════╗
//   ║ node │ rel │ source │ target ║
//   ╠══════╪═════╪════════╪════════╣
//   ║  1   │  1  │    1   │   2    ║
//   ║  2   │  2  │    2   │   3    ║
//   ╚══════╧═════╧════════╧════════╝
//  */
//
////a.schema={"node","rel","source","target"}
////b.schema={"n__NODE"}
////joinCols={"target"->"n__NODE"}
//
//
//
//class PandaTableOperator extends SimpleTableOperator{
//  override def join(a: LynxTable, b: LynxTable, joinType: JoinType, joinCols: (String, String)*): LynxTable = {
//    joinType match {
//      case InnerJoin =>
//        val (smallTable, largeTable, smallColumns, largeColumns) =
//          if (a.size < b.size) {
//            (a, b, joinCols.map(_._1), joinCols.map(_._2))
//          }
//          else {
//            (b, a, joinCols.map(_._2), joinCols.map(_._1))
//          }
//
//        val smallMap: Map[Seq[CypherValue], Iterable[(Seq[CypherValue], Seq[CypherValue])]] =
//          smallTable.records.map {
//            row => {
//              //joinCols: a.node=b.source
//              val value = smallColumns.map(joinCol => smallTable.cell(row, joinCol))
//              value -> row
//            }
//          }.groupBy(_._1)
//
//        val joinedSchema = smallTable.schema ++ largeTable.schema
//        val joinedRecords = largeTable.records.flatMap {
//          row => {
//            val value = largeColumns.map(joinCol => largeTable.cell(row, joinCol))
//            smallMap.getOrElse(value, Seq()).map(x => x._2 ++ row)
//          }
//        }
//
//        LynxTable(joinedSchema, joinedRecords)
//    }
//  }
//
//  def scanRelJoin(a: LynxTable): LynxTable = {
//    // LynxTable()  todo
//    null
//  }
//
//  def scanNodesJoin(): LynxTable = {
//    null
//  }
//
//
//
//
//}
